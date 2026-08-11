package balancer

import (
	"errors"
	"strconv"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/naming"
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
	grp := strconv.Itoa(nodeId)
	service, err := b.discoverClient.GetService(serviceName, grp, nil)
	if err != nil {
		return nil, err
	}
	if service == nil {
		return nil, errors.New("service not found")
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
