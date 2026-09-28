package dataflow

import (
	"errors"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/network"
)

func getClient() (*network.TcpClient, error) {
	mutex.RLock()
	client := cli
	mutex.RUnlock()

	if client == nil || client.IsClosed() {
		return nil, errors.New("tcp client is not initialized")
	}
	return client, nil
}

func NotifyNewStreamEvent(metric *gen.StreamMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_STREAM_ADD), metric); err != nil {
		logger.Errorf("failed to send stream add: %v, %v", metric, err)
		return err
	}
	return nil
}

func NotifyDeleteStreamEvent(metric *gen.StreamMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_STREAM_DELETE), metric); err != nil {
		logger.Errorf("failed to send stream delete: %v, %v", metric, err)
		return err
	}
	return nil
}

func NotifyStreamFailedEvent(metric *gen.StreamMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_STREAM_FAILED), metric); err != nil {
		logger.Errorf("failed to send stream failed: %v, %v", metric, err)
		return err
	}
	return nil
}

func NotifyStreamStatusEvent(metric *gen.StreamMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_STREAM_STATUS), metric); err != nil {
		logger.Errorf("failed to send stream status: %v, %v", metric, err)
		return err
	}
	return nil
}

func NotifyStreamsQueryEvent(metric *gen.StreamMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_STREAMS_QUERY_REQ), metric); err != nil {
		logger.Errorf("failed to send streams query req: %v, %v", metric, err)
		return err
	}
	return nil
}

func NotifyNewSessionEvent(metric *gen.SessionMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_SESSION_ADD), metric); err != nil {
		logger.Errorf("failed to send session add: %v, %v", metric, err)
		return err
	}
	return nil
}

func NotifyDeleteSessionEvent(metric *gen.SessionMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_SESSION_DELETE), metric); err != nil {
		logger.Errorf("failed to send session delete: %v, %v", metric, err)
		return err
	}
	return nil
}
func NotifySessionUpdateEvent(metric *gen.SessionMetric) error {
	client, err := getClient()
	if err != nil {
		return err
	}

	if err := client.Write(uint8(gen.CMD_DATA_REPORT),
		uint8(gen.SCMDDataReport_SESSION_STATUS), metric); err != nil {
		logger.Errorf("failed to send session status update: %v, %v", metric, err)
		return err
	}
	return nil
}
