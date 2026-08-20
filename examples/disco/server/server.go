package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/naming"
	"github.com/dellinger2023/net-flux/pkg/network"
	"google.golang.org/protobuf/proto"
)

var (
	addr     string
	discoCfg = &naming.DiscoSetting{
		Host:         "192.168.2.40",
		Port:         8848,
		Namespace:    "public",
		LogDir:       "./logs",
		CacheDir:     "./cache",
		PreloadCache: true,
		Timeout:      5000,
		GroupName:    "DEFAULT_GROUP",
		Username:     "nacos",
		Password:     "nacos",
		Node:         1,
	}
	factory = newDiscoClientFactory(discoCfg)
)

func main() {
	flag.StringVar(&addr, "addr", ":1911", "listen address")
	flag.StringVar(&discoCfg.Host, "disco-host", "127.0.0.1", "disco host")
	flag.IntVar(&discoCfg.Port, "disco-port", 8848, "disco port")
	flag.StringVar(&discoCfg.Namespace, "disco-namespace", "public", "disco namespace")
	flag.StringVar(&discoCfg.LogDir, "disco-log-dir", "./logs", "disco log directory")
	flag.StringVar(&discoCfg.CacheDir, "disco-cache-dir", "./cache", "disco cache directory")
	flag.BoolVar(&discoCfg.PreloadCache, "disco-preload-cache", true, "disco preload cache")
	flag.IntVar(&discoCfg.Timeout, "disco-timeout", 5000, "disco timeout")
	flag.StringVar(&discoCfg.GroupName, "disco-group", "DEFAULT_GROUP", "disco group")
	flag.StringVar(&discoCfg.Username, "disco-username", "nacos", "disco username")
	flag.StringVar(&discoCfg.Password, "disco-password", "nacos", "disco password")
	flag.IntVar(&discoCfg.Node, "disco-node", 1, "disco node")
	flag.Parse()

	// discoClient, err = naming.NewNacosDiscoverClient(discoCfg)
	// if err != nil {
	// 	logger.Fatalf("failed to create disco client: %v", err)
	// }
	// time.Sleep(time.Second)

	logger.Info("disco server is starting...")
	logger.Info("================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("disco server is shutting down...")
		cancel()
	}()

	svr := network.NewTcpServer(addr, &eventHandler{}, nil)
	if err := svr.Run(ctx); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}

type eventHandler struct{}

func (h *eventHandler) OnConnect(conn network.TCPConn) error {
	logger.Infof("new connection from %s", conn.RemoteAddr().String())
	return nil
}

func (h *eventHandler) OnClose(conn network.TCPConn) {
	logger.Infof("connection from %s closed", conn.RemoteAddr().String())
}

func (h *eventHandler) OnCmdSystem(conn network.TCPConn, pkt proto.Message) error {
	writer := conn.(network.TCPWriter)
	switch pkt := pkt.(type) {
	case *gen.Ping:
		return writer.WritePacket(
			uint8(gen.CMD_SYSTEM),
			uint8(gen.SCMDSystem_PONG),
			&gen.Pong{Timestamp: pkt.Timestamp})

	default:
		return fmt.Errorf("unknown system command: %T", pkt)
	}
}

func (h *eventHandler) OnCmdDiscovery(conn network.TCPConn, pkt proto.Message) error {
	writer := conn.(network.TCPWriter)
	switch pkt := pkt.(type) {
	case *gen.Instance:
		logger.Infof("register instance: name=%s node=%d payload=%v",
			pkt.GetInstanceName(), pkt.GetNode(), pkt)
		discoClient := factory.GetDiscoClient(int(conn.ID()), pkt.InstanceName)
		if discoClient == nil {
			return fmt.Errorf("failed to get disco client for %s", pkt.InstanceName)
		}
		if err := discoClient.RegisterInstance(pkt); err != nil {
			return err
		}
		logger.Infof("register instance success: name=%s node=%d", pkt.GetInstanceName(), pkt.GetNode())

	case *gen.Deregister:
		logger.Infof("deregister instance: [node=%d] %v", pkt.GetNode(), pkt)
		discoClient := factory.GetDiscoClient(int(conn.ID()), pkt.InstanceName)
		if discoClient == nil {
			return fmt.Errorf("failed to get disco client for %s", pkt.InstanceName)
		}
		err := discoClient.DeregisterInstance(pkt.InstanceName, strconv.Itoa(int(pkt.Node)), pkt.Ip, uint64(pkt.Port))
		if err != nil {
			logger.Errorf("failed to deregister instance: %v", err)
			return err
		}
		logger.Infof("deregister instance success: [node=%d] %v", pkt.GetNode(), pkt)
		factory.RemoveDiscoClient(int(conn.ID()), pkt.InstanceName)

	case *gen.Lookup:
		logger.Infof("lookup instance: %v", pkt)
		discoClient := factory.GetDiscoClient(int(conn.ID()), "")
		if discoClient == nil {
			return fmt.Errorf("failed to get disco client for lookup")
		}
		instances, err := discoClient.GetServiceInstances(pkt.ServiceName, strconv.Itoa(int(pkt.Node)), []string{})
		//inst, err := discoClient.GetServiceInstanceByGroup(pkt.ServiceName)
		if err != nil {
			return err
		}

		services := make([]*gen.Service, 0)
		services = append(services, &gen.Service{
			Instances: instances,
			Cluster:   "",
			Name:      pkt.ServiceName,
			GroupName: strconv.Itoa(int(pkt.Node)),
			Valid:     pkt.Healthy,
		})
		return writer.WritePacket(
			uint8(gen.CMD_DISCOVERY),
			uint8(gen.SCMDDisco_LOOKUP_ACK),
			&gen.LookupAck{Services: services})
	}
	return nil
}

func (h *eventHandler) OnCmdDataReport(conn network.TCPConn, subcmd uint8, pkt proto.Message) error {
	switch pkt.(type) {
	case *gen.MachineMetric:
		logger.Infof("machine metric subcmd=%d: %v", subcmd, pkt)
		return nil
	case *gen.NetworkMetric:
		logger.Infof("network metric subcmd=%d: %v", subcmd, pkt)
		return nil
	case *gen.StreamMetric:
		logger.Infof("stream metric subcmd=%d: %v", subcmd, pkt)
		return nil
	case *gen.SessionMetric:
		logger.Infof("session metric subcmd=%d: %v", subcmd, pkt)
		return nil
	}
	return nil
}

func (h *eventHandler) OnCmdConfig(conn network.TCPConn, pkt proto.Message) error {
	switch pkt.(type) {
	case *gen.WhiteipChanged:
		logger.Infof("whiteip changed: %v", pkt)
		return nil
	case *gen.LimitChanged:
		logger.Infof("limit changed: %v", pkt)
		return nil
	}
	return nil
}

func (h *eventHandler) OnCmdEvent(conn network.TCPConn, pkt proto.Message) error {
	switch pkt.(type) {
	case *gen.AccessDeniedEvent:
		logger.Infof("access denied event: %v", pkt)
		return nil
	case *gen.IpBlockedEvent:
		logger.Infof("ip blocked event: %v", pkt)
		return nil
	}
	return nil
}

func (h *eventHandler) OnCmdControl(conn network.TCPConn, pkt proto.Message) error {
	logger.Infof("control command: %v", pkt)
	return nil
}
