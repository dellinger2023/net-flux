package balancer

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/naming"
	"github.com/dellinger2023/net-flux/pkg/util"
)

type composeBalancer struct {
	innerBalancer  Balancer
	discoverClient naming.DiscoClient
}

// DiscoverClient implements Balancer.
func (b *composeBalancer) DiscoverClient() naming.DiscoClient {
	return b.discoverClient
}

func (b *composeBalancer) Pick(service *gen.Service) (*gen.Instance, error) {
	return b.innerBalancer.Pick(service)
}

func (b *composeBalancer) Resolve(serviceName, streamId string, nodeId int) (*gen.Instance, error) {
	if b.discoverClient == nil {
		return nil, errors.New("discover client is not set")
	}

	key := fmt.Sprintf("service_%s_%s_%d", serviceName, streamId, nodeId)
	content, err := b.discoverClient.GetConfig(key)
	if err != nil {
		return nil, err
	}
	if util.IsEmptyStr(content) {
		instance, err := b.innerBalancer.Resolve(serviceName, streamId, nodeId)
		if err != nil {
			return nil, err
		}
		buf, err := json.Marshal(instance)
		if err != nil {
			return nil, err
		}
		_ = b.discoverClient.SetConfig(key, string(buf))
		return instance, nil
	}

	var instance gen.Instance
	if err = json.Unmarshal([]byte(content), &instance); err != nil {
		return nil, err
	}
	return &instance, nil
}

func NewComposeBalancer(innerBalancer Balancer, discoverClient naming.DiscoClient) Balancer {
	return &composeBalancer{innerBalancer: innerBalancer, discoverClient: discoverClient}
}
