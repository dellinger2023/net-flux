package network

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
)

type TcpClient struct {
	addr    string
	event   EventHandler
	options *TCPConnOptions
	conn    TCPConn
}

func NewTcpClient(addr string, event EventHandler, options *TCPConnOptions) (*TcpClient, error) {
	opts := TCPConnOptions{}
	if options != nil {
		opts = *options
	}
	opts.Handler = event

	tcpConn, err := NewClientConn(opts)
	if err != nil {
		return nil, err
	}

	return &TcpClient{
		addr:    addr,
		event:   event,
		options: options,
		conn:    tcpConn,
	}, nil
}

func (c *TcpClient) Connect() error {
	return c.conn.Connect(c.addr)
}

// Run connects and blocks until ctx is canceled, then closes the connection.
func (c *TcpClient) Run(ctx context.Context) error {
	if err := c.Connect(); err != nil {
		return err
	}
	return c.Wait(ctx)
}

// Wait blocks until ctx is canceled, then closes the connection.
func (c *TcpClient) Wait(ctx context.Context) error {
	<-ctx.Done()
	return c.shutdown(ctx.Err())
}

func (c *TcpClient) Close() error {
	return c.shutdown(nil)
}

func (c *TcpClient) shutdown(cause error) error {
	err := c.conn.Close()
	if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrNoReady) {
		return err
	}
	if cause != nil {
		return cause
	}
	return nil
}

func (c *TcpClient) IsClosed() bool {
	return c.conn.IsClosed()
}

func (c *TcpClient) Write(cmd, subcmd uint8, pkt proto.Message) error {
	return c.conn.WritePacket(cmd, subcmd, pkt)
}

// SetEventHandler 替换事件处理器（可用于挂载 LOOKUP_ACK 等待逻辑）。
func (c *TcpClient) SetEventHandler(handler EventHandler) {
	c.event = handler
	if options := c.options; options != nil {
		options.Handler = handler
	}
	if tc, ok := c.conn.(*tcpConn); ok {
		tc.event = handler
		if tc.handler == tc {
			// connHandler 仍是自身，OnMessage 会转调 tc.event
		}
	}
}

// EventHandler 返回当前事件处理器。
func (c *TcpClient) EventHandler() EventHandler {
	return c.event
}
