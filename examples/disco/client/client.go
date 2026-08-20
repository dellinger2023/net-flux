package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/dataflow"
	"github.com/dellinger2023/net-flux/pkg/dataflow/core"
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/network"
	"github.com/dellinger2023/net-flux/pkg/util"
	"google.golang.org/protobuf/proto"
)

var (
	addr   string
	node   int = 1
	slogan     = `
	=============================
	NetFlux Discovery Client
	=============================
	1. 注册服务
	2. 注销服务
	3. 查询服务
	4. 上报数据
	5. 上报配置
	6. 上报事件
	7. 上报控制
	=============================
	请选择:`
)

func main() {
	flag.StringVar(&addr, "addr", "127.0.0.1:1911", "disco server address")
	flag.IntVar(&node, "node", 1, "机房节点 ID")
	flag.Parse()

	cli, err := network.NewTcpClient(addr, &eventHandler{}, nil)
	if err != nil {
		logger.Fatalf("failed to create disco client: %v", err)
	}

	if err := cli.Connect(); err != nil {
		logger.Fatalf("failed to connect to disco server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("disco client is shutting down...")
		cancel()
	}()

	go interactiveLoop(ctx, cli)

	if err := cli.Wait(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Errorf("client exited: %v", err)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func interactiveLoop(ctx context.Context, cli *network.TcpClient) {
	for ctx.Err() == nil {
		logger.Info(slogan)
		var choice int
		if _, err := fmt.Scanln(&choice); err != nil {
			return
		}
		switch choice {
		case 1:
			logger.Info("注册服务")
			instance := &gen.Instance{
				InstanceName: "test1",
				PublicIp:     "124.79.229.235",
				PublicPort:   80,
				PrivateIp:    "127.0.0.1",
				PrivatePort:  8080,
				InnerIp:      "10.0.0.1",
				InnerPort:    9000,
				Weight:       1.0,
				Healthy:      true,
				Enable:       true,
				Ephemeral:    false,
				Node:         int32(node),
			}
			logger.Infof("send register, node=%d", instance.GetNode())
			if err := cli.Write(uint8(gen.CMD_DISCOVERY), uint8(gen.SCMDDisco_REGISTER), instance); err != nil {
				logger.Errorf("register write failed: %v", err)
			}

			instance1 := &gen.Instance{
				InstanceName: "test2",
				PublicIp:     "124.79.229.234",
				PublicPort:   80,
				PrivateIp:    "127.0.0.2",
				PrivatePort:  8080,
				InnerIp:      "10.0.0.2",
				InnerPort:    9000,
				Weight:       1.0,
				Healthy:      true,
				Enable:       true,
				Ephemeral:    false,
				Node:         int32(node),
			}
			logger.Infof("send register1, node=%d", instance1.GetNode())
			if err := cli.Write(uint8(gen.CMD_DISCOVERY), uint8(gen.SCMDDisco_REGISTER), instance1); err != nil {
				logger.Errorf("register1 write failed: %v", err)
			}

		case 2:
			logger.Info("注销服务")
			deregister := &gen.Deregister{
				InstanceName: "test1",
				Ip:           "127.0.0.1",
				Port:         8080,
				Node:         int32(node),
			}
			if err := cli.Write(uint8(gen.CMD_DISCOVERY), uint8(gen.SCMDDisco_DEREGISTER), deregister); err != nil {
				logger.Errorf("deregister write failed: %v", err)
			}

			deregister2 := &gen.Deregister{
				InstanceName: "test2",
				Ip:           "127.0.0.2",
				Port:         8080,
				Node:         int32(node),
			}
			if err := cli.Write(uint8(gen.CMD_DISCOVERY), uint8(gen.SCMDDisco_DEREGISTER), deregister2); err != nil {
				logger.Errorf("deregister write failed: %v", err)
			}

		case 3:
			logger.Info("查询服务")
			lookup := &gen.Lookup{
				ServiceName: "test",
				Node:        1,
				Healthy:     true,
			}
			if err := cli.Write(uint8(gen.CMD_DISCOVERY), uint8(gen.SCMDDisco_LOOKUP), lookup); err != nil {
				logger.Errorf("lookup write failed: %v", err)
			}

		case 4:
			logger.Info("上报数据")
			nodeInfo, err := core.ReadBaseNodeInfo(node)
			if err != nil {
				logger.Errorf("read base node info failed: %v", err)
				return
			}
			cfg := &core.DataflowConfig{
				NodeInfo: &core.NodeInfo{
					ID:        nodeInfo.ID,
					IP:        nodeInfo.IP,
					Port:      nodeInfo.Port,
					Node:      node,
					Version:   nodeInfo.Version,
					Status:    nodeInfo.Status,
					StartTime: time.Now().Unix(),
				},
				ProbeAddresses: []string{
					"www.baidu.com",    // 国内，网速快
					"www.ludechem.com", // 国外，网速慢
					"www.google.com",   // 国外，需要FQ
				},
				ReportMachineInterval: 10 * time.Second,
				ReportNetworkInterval: 10 * time.Second,
			}
			if err := dataflow.Initialize(cli, cfg); err != nil {
				logger.Errorf("dataflow initialize failed: %v", err)
				break
			}

			streamCtx, streamCancel := context.WithCancel(context.Background())
			go func() {
				defer streamCancel()
				for streamCtx.Err() == nil {
					streamId := util.NewUUID(true)
					if !sleepCtx(streamCtx, time.Second) {
						return
					}
					if err := dataflow.NotifyNewStreamEvent(&gen.StreamMetric{
						MachineId:   nodeInfo.ID,
						StreamId:    streamId,
						StreamPath:  "/vod/" + streamId + ".m3u8",
						MachineType: gen.MachineType_MT_GATEWAY,
						Status:      gen.StreamStatus_SS_RUNNING,
						VideoCodec:  gen.StreamCodec_SC_H264,
						AudioCodec:  gen.StreamCodec_SC_AAC,
						Protocol:    gen.StreamProtocol_SP_HTTP_FLV,
						Bitrate:     1000000,
						Width:       1920,
						Height:      1080,
						StreamAlias: "/liveshow",
						Timestamp:   time.Now().Unix(),
						NodeId:      strconv.Itoa(node),
					}); err != nil {
						logger.Errorf("notify new stream failed: %v", err)
					}
					if !sleepCtx(streamCtx, time.Second) {
						return
					}
					if err := dataflow.NotifyDeleteStreamEvent(&gen.StreamMetric{
						MachineId:   nodeInfo.ID,
						StreamId:    streamId,
						StreamPath:  "/vod/" + streamId + ".m3u8",
						MachineType: gen.MachineType_MT_GATEWAY,
						Status:      gen.StreamStatus_SS_CLOSED,
					}); err != nil {
						logger.Errorf("notify delete stream failed: %v", err)
					}
					if !sleepCtx(streamCtx, time.Second) {
						return
					}
					if err := dataflow.NotifyStreamFailedEvent(&gen.StreamMetric{
						MachineId:   nodeInfo.ID,
						StreamId:    streamId,
						StreamPath:  "/vod/" + streamId + ".m3u8",
						MachineType: gen.MachineType_MT_GATEWAY,
						Status:      gen.StreamStatus_SS_FAILED,
					}); err != nil {
						logger.Errorf("notify stream failed event failed: %v", err)
					}
					if !sleepCtx(streamCtx, time.Second) {
						return
					}
					if err := dataflow.NotifyStreamStatusEvent(&gen.StreamMetric{
						MachineId:   nodeInfo.ID,
						StreamId:    streamId,
						StreamPath:  "/vod/" + streamId + ".m3u8",
						MachineType: gen.MachineType_MT_GATEWAY,
					}); err != nil {
						logger.Errorf("notify stream status failed: %v", err)
					}
					if !sleepCtx(streamCtx, time.Second) {
						return
					}
					if err := dataflow.NotifyStreamsQueryEvent(&gen.StreamMetric{
						MachineId:   nodeInfo.ID,
						StreamId:    streamId,
						StreamPath:  "/vod/" + streamId + ".m3u8",
						MachineType: gen.MachineType_MT_GATEWAY,
					}); err != nil {
						logger.Errorf("notify streams query failed: %v", err)
					}
				}
			}()

			time.Sleep(30 * time.Second)
			streamCancel()
			if err := dataflow.Shutdown(); err != nil {
				logger.Errorf("dataflow shutdown failed: %v", err)
			}
		default:
			logger.Info("无效的选择")
		}
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
	logger.Infof("system command: %v", pkt)
	switch pkt := pkt.(type) {
	case *gen.Pong:
		logger.Infof("RTT = %vms", time.Since(time.Unix(pkt.Timestamp, 0)).Milliseconds())
		return nil
	default:
		return fmt.Errorf("unknown system command: %T", pkt)
	}
}

func (h *eventHandler) OnCmdDiscovery(conn network.TCPConn, pkt proto.Message) error {
	logger.Infof("discovery command: %v", pkt)
	return nil
}

func (h *eventHandler) OnCmdDataReport(conn network.TCPConn, subcmd uint8, pkt proto.Message) error {
	logger.Infof("data report command subcmd=%d: %v", subcmd, pkt)
	return nil
}
func (h *eventHandler) OnCmdConfig(conn network.TCPConn, pkt proto.Message) error {
	logger.Infof("config command: %v", pkt)
	return nil
}

func (h *eventHandler) OnCmdEvent(conn network.TCPConn, pkt proto.Message) error {
	logger.Infof("event command: %v", pkt)
	return nil
}

func (h *eventHandler) OnCmdControl(conn network.TCPConn, pkt proto.Message) error {
	logger.Infof("control command: %v", pkt)
	return nil
}
