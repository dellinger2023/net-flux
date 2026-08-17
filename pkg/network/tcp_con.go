package network

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/network/misc"
	"google.golang.org/protobuf/proto"
)

type tcpConn struct {
	conn         net.Conn
	id           uint32
	remoteAddr   string
	closed       atomic.Bool
	skipOutgoing atomic.Bool
	closeOnce    sync.Once
	closeChan    chan struct{}
	options      TCPConnOptions
	handler      connHandler
	event        EventHandler
	reader       TCPReader
	writer       TCPWriter
	writeMu      sync.Mutex
	outgoingCh   chan TCPConn
}

func NewServerConn(conn net.Conn, opts TCPConnOptions) (TCPConn, error) {
	c := &tcpConn{
		conn:       conn,
		id:         opts.ID,
		options:    opts,
		event:      opts.Handler,
		outgoingCh: opts.OutgoingCh,
	}
	c.handler = c
	if err := c.initIO(); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

func NewClientConn(opts TCPConnOptions) (TCPConn, error) {
	c := &tcpConn{
		conn:       nil,
		id:         opts.ID,
		options:    opts,
		event:      opts.Handler,
		outgoingCh: opts.OutgoingCh,
	}
	c.handler = c
	return c, nil
}

func (c *tcpConn) ID() uint32 {
	return c.id
}

func (c *tcpConn) RemoteAddr() net.Addr {
	if c.conn == nil {
		return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}
	return c.conn.RemoteAddr()
}

func (c *tcpConn) LocalAddr() net.Addr {
	if c.conn == nil {
		return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}
	return c.conn.LocalAddr()
}

func (c *tcpConn) SetReadDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNoReady
	}
	return c.conn.SetReadDeadline(t)
}

func (c *tcpConn) SetWriteDeadline(t time.Time) error {
	if c.conn == nil {
		return ErrNoReady
	}
	return c.conn.SetWriteDeadline(t)
}

func (c *tcpConn) SetHeartbeatInterval(d time.Duration) error {
	if c.conn == nil {
		return ErrNoReady
	}
	c.options.HeartbeatInterval = d
	return nil
}

// NoNagle is the option of tcp connection.
// If enable is true, the tcp connection will use the no nagle algorithm.
// default is false.
func (c *tcpConn) SetNoNagle(enable bool) error {
	if c.conn == nil {
		return ErrNoReady
	}
	return c.conn.(*net.TCPConn).SetNoDelay(enable)
}

// SetKeepAlive is the option of tcp connection.
// If enable is true, the tcp connection will use the keep alive.
// default is false.
func (c *tcpConn) SetKeepAlive(enable bool) error {
	if c.conn == nil {
		return ErrNoReady
	}
	return c.conn.(*net.TCPConn).SetKeepAlive(enable)
}

// SetKeepAlivePeriod is the option of tcp connection.
// If d is 0, the tcp connection will use the default keep alive period.
// default is 0.
func (c *tcpConn) SetKeepAlivePeriod(d time.Duration) error {
	if c.conn == nil {
		return ErrNoReady
	}
	return c.conn.(*net.TCPConn).SetKeepAlivePeriod(d)
}

// SetReuseAddr is the option of tcp connection.
// If enable is true, the tcp connection will use the reuse addr.
// default is false.
func (c *tcpConn) SetReuseAddr(enable bool) error {
	v := 0
	if enable {
		v = 1
	}
	return c.tcpControl(func(fd uintptr) error {
		return sysSetsockoptReuseAddr(fd, v)
	})
}

// SetReusePort is the option of tcp connection.
// If enable is true, the tcp connection will use the reuse port.
// default is false.
func (c *tcpConn) SetReusePort(enable bool) error {
	v := 0
	if enable {
		v = 1
	}
	return c.tcpControl(func(fd uintptr) error {
		return sysSetsockoptReusePort(fd, v)
	})
}

// tcpControl is the helper function to control the tcp connection.
// It will return the error if the connection is not *net.TCPConn.
func (c *tcpConn) tcpControl(fn func(fd uintptr) error) error {
	if c.conn == nil {
		return ErrNoReady
	}
	tcp, ok := c.conn.(*net.TCPConn)
	if !ok {
		return errors.New("netx: connection is not *net.TCPConn")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	var ctlErr error
	err = raw.Control(func(fd uintptr) {
		ctlErr = fn(fd)
	})
	if err != nil {
		return err
	}
	return ctlErr
}

func (c *tcpConn) RawConn() net.Conn {
	return c.conn
}

func (c *tcpConn) Write(p []byte) (n int, err error) {
	if c.conn == nil || c.closed.Load() {
		return 0, ErrClosed
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeAll(c.conn, p)
}

func (c *tcpConn) Read(p []byte) (n int, err error) {
	if c.conn == nil || c.closed.Load() {
		return 0, ErrClosed
	}
	return c.conn.Read(p)
}

func (c *tcpConn) initIO() error {
	if c.closeChan == nil {
		c.closeChan = make(chan struct{})
	}
	var err error
	c.writer, err = newWriter(c)
	if err != nil {
		return err
	}
	if c.event != nil {
		if err := c.handler.OnConnect(c); err != nil {
			return err
		}
		c.reader, err = newReader(c, c.handler)
		return err
	}
	return nil
}

func (c *tcpConn) Connect(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	c.conn = conn
	c.remoteAddr = addr
	c.closed.Store(false)
	if err := c.initIO(); err != nil {
		conn.Close()
		c.conn = nil
		return err
	}
	if c.options.HeartbeatInterval > 0 {
		go c.heartbeat()
	}
	return nil
}

func (c *tcpConn) heartbeat() {

	ticker := time.NewTicker(c.options.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if c.writer == nil || c.closed.Load() {
				continue
			}

			_ = c.writer.WritePacket(uint8(gen.CMD_SYSTEM), uint8(gen.SCMDSystem_PING),
				&gen.Ping{Timestamp: time.Now().Unix()})
		case <-c.closeChan:
			return
		}
	}

}

func (c *tcpConn) ConnectWithTLS(addr string, tlsConfig *tls.Config) error {
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return err
	}
	c.conn = conn
	c.remoteAddr = addr
	c.closed.Store(false)
	if err := c.initIO(); err != nil {
		conn.Close()
		c.conn = nil
		return err
	}
	if c.options.HeartbeatInterval > 0 {
		go c.heartbeat()
	}
	return nil
}

func (c *tcpConn) Close() error {
	return c.close(false)
}

// abortClose forcefully closes the connection without blocking shutdown.
// Server shutdown uses skipOutgoing=true to avoid deadlocking the accept loop.
func (c *tcpConn) abortClose(skipOutgoing bool) error {
	if skipOutgoing {
		c.skipOutgoing.Store(true)
	}
	return c.close(true)
}

func (c *tcpConn) close(abort bool) error {
	var err error
	c.closeOnce.Do(func() {
		if c.conn == nil {
			err = ErrNoReady
			return
		}
		c.closed.Store(true)
		if c.closeChan != nil {
			close(c.closeChan)
		}
		if c.handler != nil {
			c.handler.OnClose(c)
		}

		conn := c.conn
		c.conn = nil
		if abort {
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
				_ = tc.CloseWrite()
			}
			past := time.Now()
			_ = conn.SetReadDeadline(past)
			_ = conn.SetWriteDeadline(past)
			go func() { _ = conn.Close() }()
		} else {
			err = conn.Close()
		}

		if abort && c.options.ReconnectInterval > 0 && c.outgoingCh == nil && c.remoteAddr != "" {
			go c.reconnect()
		}
	})
	return err
}

func (c *tcpConn) reconnect() {
	addr := c.remoteAddr
	if addr == "" {
		logger.Errorf("[id=%d] reconnect skipped: empty remote addr", c.id)
		return
	}

	for i := 0; c.options.ReconnectMax == 0 || i < c.options.ReconnectMax; i++ {
		time.Sleep(c.options.ReconnectInterval)
		logger.Infof("[id=%d] reconnecting to %s (attempt %d)", c.id, addr, i+1)

		conn, dialErr := net.Dial("tcp", addr)
		if dialErr != nil {
			logger.Errorf("[id=%d] reconnect dial failed: %v", c.id, dialErr)
			continue
		}

		c.writeMu.Lock()
		c.conn = conn
		c.closed.Store(false)
		c.closeOnce = sync.Once{}
		c.closeChan = make(chan struct{})
		c.writeMu.Unlock()

		if initErr := c.initIO(); initErr != nil {
			logger.Errorf("[id=%d] reconnect initIO failed: %v", c.id, initErr)
			_ = conn.Close()
			c.writeMu.Lock()
			c.conn = nil
			c.closed.Store(true)
			c.writeMu.Unlock()
			continue
		}

		if c.options.HeartbeatInterval > 0 {
			go c.heartbeat()
		}
		logger.Infof("[id=%d] reconnected to %s", c.id, addr)
		return
	}
	logger.Errorf("[id=%d] reconnect gave up to %s", c.id, addr)
}

func (c *tcpConn) IsClosed() bool {
	return c.closed.Load()
}

func (c *tcpConn) OnConnect(conn TCPConn) error {
	if c.event == nil {
		return nil
	}
	return c.event.OnConnect(conn)
}

func (c *tcpConn) OnClose(conn TCPConn) {
	if c.event != nil {
		c.event.OnClose(conn)
	}
	if c.outgoingCh != nil && !c.skipOutgoing.Load() {
		select {
		case c.outgoingCh <- conn:
		default:
		}
	}
}

func (c *tcpConn) OnMessage(conn TCPConn, head *PacketHeader, data []byte) error {
	if c.event == nil {
		return nil
	}
	if c.options.ParentCtx != nil {
		select {
		case <-c.options.ParentCtx.Done():
			return c.options.ParentCtx.Err()
		default:
		}
	}
	switch head.CMD {
	case uint8(gen.CMD_SYSTEM):
		pkt, err := misc.UnmarshalSystem(head.SubCMD, data)
		if err != nil {
			logger.Errorf("[id=%d] unmarshal system error: %v", conn.ID(), err)
			return err
		}
		return c.event.OnCmdSystem(c, pkt)
	case uint8(gen.CMD_DISCOVERY):
		pkt, err := misc.UnmarshalDiscovery(head.SubCMD, data)
		if err != nil {
			logger.Errorf("[id=%d] unmarshal discovery error: %v", conn.ID(), err)
			return err
		}
		return c.event.OnCmdDiscovery(c, pkt)
	case uint8(gen.CMD_DATA_REPORT):
		pkt, err := misc.UnmarshalDataReport(head.SubCMD, data)
		if err != nil {
			logger.Errorf("[id=%d] unmarshal data report error: %v", conn.ID(), err)
			return err
		}
		return c.event.OnCmdDataReport(c, head.SubCMD, pkt)
	case uint8(gen.CMD_CONFIG):
		pkt, err := misc.UnmarshalConfig(head.SubCMD, data)
		if err != nil {
			logger.Errorf("[id=%d] unmarshal config error: %v", conn.ID(), err)
			return err
		}
		return c.event.OnCmdConfig(c, pkt)
	case uint8(gen.CMD_EVENT):
		pkt, err := misc.UnmarshalEvent(head.SubCMD, data)
		if err != nil {
			logger.Errorf("[id=%d] unmarshal event error: %v", conn.ID(), err)
			return err
		}
		return c.event.OnCmdEvent(c, pkt)
	case uint8(gen.CMD_CONTROL):
		pkt, err := misc.UnmarshalControl(head.SubCMD, data)
		if err != nil {
			logger.Errorf("[id=%d] unmarshal control error: %v", conn.ID(), err)
			return err
		}
		return c.event.OnCmdControl(c, pkt)
	default:
		return fmt.Errorf("unknown command: %d,%d", head.CMD, head.SubCMD)
	}
}

func (c *tcpConn) WritePacket(cmd, subcmd uint8, pkt proto.Message) error {
	return c.writer.WritePacket(cmd, subcmd, pkt)
}
