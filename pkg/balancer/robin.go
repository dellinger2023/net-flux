package balancer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/naming"
	"github.com/dellinger2023/net-flux/pkg/util"
)

type roundRobinBalancer struct {
	nextIndex      int
	totalWeight    int
	groupName      string
	discoverClient naming.DiscoClient
}

func (b *roundRobinBalancer) Pick(service *gen.Service) (*gen.Instance, error) {
	if service == nil || len(service.Instances) == 0 {
		return nil, errors.New("no hosts available")
	}

	totalWeight := 0
	for _, host := range service.Instances {
		totalWeight += int(host.Weight)
	}

	b.totalWeight = totalWeight
	b.nextIndex = (b.nextIndex + 1) % len(service.Instances)

	return service.Instances[b.nextIndex], nil
}

func (b *roundRobinBalancer) Resolve(serviceName, streamId string, nodeId int) (*gen.Instance, error) {
	var service *gen.Service
	var err error
	grp := strconv.Itoa(nodeId)
	key := fmt.Sprintf("service_%s_%d", serviceName, nodeId)
	content, err := b.discoverClient.GetConfig(key)
	if err != nil {
		return nil, err
	}

	if util.IsEmptyStr(content) {
		service, err = b.discoverClient.GetService(serviceName, grp, nil)
		if err != nil {
			logger.Errorf("get service failed: %v", err)
			return nil, err
		}

		if service == nil {
			return nil, errors.New("service not found1")
		}

		buff, err := json.Marshal(service)
		if err != nil {
			logger.Errorf("marshal service failed: %v", err)
			return nil, err
		}

		err = b.discoverClient.SetConfig(key, string(buff))
		if err != nil {
			logger.Errorf("set config failed1: %v", err)
			return nil, err
		}
	} else {
		err = json.Unmarshal([]byte(content), &service)
		if err != nil {
			logger.Errorf("unmarshal service failed: %v", err)
			return nil, err
		}
	}

	if service == nil {
		return nil, errors.New("service not found2")
	}

	err = b.discoverClient.SetConfig(key, service.String())
	if err != nil {
		logger.Errorf("set config failed2: %v", err)
		return nil, err
	}

	return b.Pick(service)
}

func NewRoundRobinBalancer(cli naming.DiscoClient) Balancer {
	return &roundRobinBalancer{
		nextIndex:      0,
		totalWeight:    0,
		groupName:      cli.GetGroupName(),
		discoverClient: cli,
	}
}
