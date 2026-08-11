package naming

import (
	"errors"
	"strconv"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/network"
	"github.com/dellinger2023/net-flux/pkg/util/cache"
	"github.com/dellinger2023/net-flux/pkg/util/redis"
)

const (
	ConfigCachePrefixKey = "mb:disco:"
)

type rpcClient struct {
	cli       *network.TcpClient
	redisCli  *redis.Client
	storage   cache.Cache
	groupName string
}

// CancelListenConfig implements DiscoClient.
func (r *rpcClient) CancelListenConfig(dataId string) error {
	panic("unimplemented")
}

// Close implements DiscoClient.
func (r *rpcClient) Close() {
	if r.storage != nil {
		r.storage.Close()
	}
	if r.redisCli != nil {
		r.redisCli.Close()
	}
}

// DeleteConfig implements DiscoClient.
func (r *rpcClient) DeleteConfig(dataId string) error {
	if r.storage == nil {
		return errors.New("storage is nil")
	}

	return r.storage.Delete(dataId)
}

// DeregisterInstance implements DiscoClient.
func (r *rpcClient) DeregisterInstance(serviceName string, groupName string, ip string, port uint64) error {
	if r.cli == nil {
		return errors.New("tcp client is nil")
	}

	logger.Debugf("send deregister, ip=%s, port=%d", ip, port)
	node, err := strconv.Atoi(r.groupName)
	if err != nil {
		return errors.New("invalid group name")
	}
	deregister := &gen.Deregister{
		InstanceName: serviceName,
		Node:         int32(node),
		Ip:           ip,
		Port:         int32(port),
	}
	if err := r.cli.Write(uint8(gen.CMD_DISCOVERY),
		uint8(gen.SCMDDisco_DEREGISTER),
		deregister); err != nil {
		logger.Errorf("deregister write failed: %v", err)
		return err
	}

	return nil
}

// GetAllServices implements DiscoClient.
func (r *rpcClient) GetAllServices(groupName string) ([]string, error) {
	panic("unimplemented")
}

// GetConfig implements DiscoClient.
func (r *rpcClient) GetConfig(dataId string) (string, error) {
	if r.storage == nil {
		return "", errors.New("storage is nil")
	}

	data, err := r.storage.Get(dataId)
	if err != nil && err != redis.ErrNil {
		logger.Errorf("get config failed: %v", err)
		return "", err
	}

	if data == nil {
		return "", errors.New("no config found:" + dataId)
	}

	return data.(string), nil
}

// GetGroupName implements DiscoClient.
func (r *rpcClient) GetGroupName() string {
	panic("unimplemented")
}

// GetService implements DiscoClient.
func (r *rpcClient) GetService(serviceName string, groupName string, clusters []string) (*gen.Service, error) {
	if r.cli == nil {
		return nil, errors.New("tcp client is nil")
	}

	logger.Debugf("send lookup, serviceName=%s", serviceName)
	lookup := &gen.Lookup{
		ServiceName: serviceName,
	}
	if err := r.cli.Write(uint8(gen.CMD_DISCOVERY),
		uint8(gen.SCMDDisco_LOOKUP),
		lookup); err != nil {
		logger.Errorf("lookup write failed: %v", err)
		return nil, err
	}

	return nil, nil
}

// GetServiceInstance implements DiscoClient.
func (r *rpcClient) GetServiceInstance(serviceName string, groupName string, clusters []string) (*gen.Instance, error) {
	_, err := r.GetService(serviceName, groupName, nil)
	if err != nil {
		logger.Errorf("GetServiceInstance failed: %v", err)
		return nil, err
	}
	return nil, nil
}

// GetServiceInstanceByGroup implements DiscoClient.
func (r *rpcClient) GetServiceInstanceByGroup(serviceName string, groupName string) (*gen.Instance, error) {
	_, err := r.GetService(serviceName, groupName, nil)
	if err != nil {
		logger.Errorf("GetServiceInstanceByGroup failed: %v", err)
		return nil, err
	}
	return nil, nil
}

// GetServiceInstanceByName implements DiscoClient.
func (r *rpcClient) GetServiceInstanceByName(serviceName string) (*gen.Instance, error) {
	_, err := r.GetService(serviceName, r.groupName, nil)
	if err != nil {
		logger.Errorf("GetServiceInstanceByName failed: %v", err)
		return nil, err
	}
	return nil, nil
}

// GetServiceInstances implements DiscoClient.
func (r *rpcClient) GetServiceInstances(serviceName string, groupName string, clusters []string) ([]*gen.Instance, error) {
	panic("unimplemented")
}

// GetServiceInstancesByName implements DiscoClient.
func (r *rpcClient) GetServiceInstancesByName(serviceName string) ([]*gen.Instance, error) {
	panic("unimplemented")
}

// ListenConfig implements DiscoClient.
func (r *rpcClient) ListenConfig(dataId string, onChange func(namespace string, group string, dataId string, data string)) error {
	panic("unimplemented")
}

// RegisterInstance implements DiscoClient.
func (r *rpcClient) RegisterInstance(instance *gen.Instance) error {
	if r.cli == nil {
		return errors.New("tcp client is nil")
	}

	logger.Debugf("send register, node=%d", instance.GetNode())
	if err := r.cli.Write(uint8(gen.CMD_DISCOVERY),
		uint8(gen.SCMDDisco_REGISTER),
		instance); err != nil {
		logger.Errorf("register write failed: %v", err)
		return err
	}

	return nil
}

// SearchConfig implements DiscoClient.
func (r *rpcClient) SearchConfig(search string, dataId string) (*ConfigPage, error) {
	panic("unimplemented")
}

// SetConfig implements DiscoClient.
func (r *rpcClient) SetConfig(dataId string, content string) error {
	if r.storage == nil {
		return errors.New("storage is nil")
	}

	return r.storage.Set(dataId, content)
}

func NewRPCDiscoveryClient(cli *network.TcpClient, redisCli *redis.Client, groupName string) (DiscoClient, error) {

	var storage cache.Cache
	var err error
	if cli == nil {
		return nil, errors.New("tcp client is nil")
	}

	if redisCli == nil {
		return nil, errors.New("redisCli is nil")
	} else {
		storage, err = cache.NewSharedCache(redisCli, ConfigCachePrefixKey)
		if err != nil {
			return nil, err
		}
	}

	return &rpcClient{
		cli:       cli,
		storage:   storage,
		groupName: groupName,
	}, nil
}
