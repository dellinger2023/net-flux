package naming

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dellinger2023/net-flux/gen"
	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/network"
	"github.com/dellinger2023/net-flux/pkg/util/cache"
	"github.com/dellinger2023/net-flux/pkg/util/redis"
	"google.golang.org/protobuf/proto"
)

const (
	ConfigCachePrefixKey = "mb:disco:"
	defaultLookupTimeout = 5 * time.Second
)

// DiscoveryResponseHandler 由基于 TCP 异步协议的 DiscoClient 实现，
// 用于接收服务端推送的 LOOKUP_ACK。
type DiscoveryResponseHandler interface {
	HandleLookupAck(ack *gen.LookupAck)
}

type lookupResult struct {
	services []*gen.Service
	err      error
}

type rpcClient struct {
	cli       *network.TcpClient
	redisCli  *redis.Client
	storage   cache.Cache
	groupName string
	timeout   time.Duration

	mu      sync.Mutex
	pending map[string]chan *lookupResult
}

func (r *rpcClient) CancelListenConfig(dataId string) error {
	return errors.New("unimplemented")
}

func (r *rpcClient) Close() {
	r.mu.Lock()
	for key, ch := range r.pending {
		select {
		case ch <- &lookupResult{err: errors.New("rpc client closed")}:
		default:
		}
		delete(r.pending, key)
	}
	r.mu.Unlock()

	if r.storage != nil {
		_ = r.storage.Close()
	}
	if r.redisCli != nil {
		_ = r.redisCli.Close()
	}
}

func (r *rpcClient) DeleteConfig(dataId string) error {
	if r.storage == nil {
		return errors.New("storage is nil")
	}
	return r.storage.Delete(dataId)
}

func (r *rpcClient) DeregisterInstance(serviceName string, groupName string, ip string, port uint64) error {
	if r.cli == nil {
		return errors.New("tcp client is nil")
	}

	logger.Debugf("send deregister, ip=%s, port=%d", ip, port)
	node, err := strconv.Atoi(groupName)
	if err != nil {
		node, err = strconv.Atoi(r.groupName)
		if err != nil {
			return errors.New("invalid group name")
		}
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

func (r *rpcClient) GetAllServices(groupName string) ([]string, error) {
	return nil, errors.New("unimplemented")
}

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
	s, ok := data.(string)
	if !ok {
		return "", fmt.Errorf("config type mismatch for %s: %T", dataId, data)
	}
	return s, nil
}

func (r *rpcClient) GetGroupName() string {
	return r.groupName
}

// GetService 发送 LOOKUP 并阻塞等待 LOOKUP_ACK（由 HandleLookupAck 唤醒）。
func (r *rpcClient) GetService(serviceName string, groupName string, clusters []string) (*gen.Service, error) {
	services, err := r.lookup(serviceName, groupName)
	if err != nil {
		return nil, err
	}
	for _, svc := range services {
		if svc != nil && svc.GetName() == serviceName {
			return svc, nil
		}
	}
	if len(services) > 0 {
		return services[0], nil
	}
	return nil, fmt.Errorf("service not found: %s", serviceName)
}

func (r *rpcClient) GetServiceInstance(serviceName string, groupName string, clusters []string) (*gen.Instance, error) {
	svc, err := r.GetService(serviceName, groupName, clusters)
	if err != nil {
		return nil, err
	}
	return pickInstance(svc)
}

func (r *rpcClient) GetServiceInstanceByGroup(serviceName string, groupName string) (*gen.Instance, error) {
	return r.GetServiceInstance(serviceName, groupName, nil)
}

func (r *rpcClient) GetServiceInstanceByName(serviceName string) (*gen.Instance, error) {
	return r.GetServiceInstance(serviceName, r.groupName, nil)
}

func (r *rpcClient) GetServiceInstances(serviceName string, groupName string, clusters []string) ([]*gen.Instance, error) {
	svc, err := r.GetService(serviceName, groupName, clusters)
	if err != nil {
		return nil, err
	}
	if svc == nil {
		return nil, fmt.Errorf("service not found: %s", serviceName)
	}
	return svc.GetInstances(), nil
}

func (r *rpcClient) GetServiceInstancesByName(serviceName string) ([]*gen.Instance, error) {
	return r.GetServiceInstances(serviceName, r.groupName, nil)
}

func (r *rpcClient) ListenConfig(dataId string, onChange func(namespace string, group string, dataId string, data string)) error {
	return errors.New("unimplemented")
}

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

func (r *rpcClient) SearchConfig(search string, dataId string) (*ConfigPage, error) {
	return nil, errors.New("unimplemented")
}

func (r *rpcClient) SetConfig(dataId string, content string) error {
	if r.storage == nil {
		return errors.New("storage is nil")
	}
	return r.storage.Set(dataId, content)
}

// HandleLookupAck 由 TCP EventHandler 在收到 LOOKUP_ACK 时调用，唤醒等待中的 GetService。
func (r *rpcClient) HandleLookupAck(ack *gen.LookupAck) {
	if ack == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	delivered := make(map[string]struct{})
	for _, svc := range ack.GetServices() {
		if svc == nil {
			continue
		}
		keys := []string{
			lookupKey(svc.GetName(), svc.GetGroupName()),
			lookupKey(svc.GetName(), r.groupName),
			lookupKey(svc.GetName(), ""),
		}
		for _, key := range keys {
			if _, ok := delivered[key]; ok {
				continue
			}
			ch, ok := r.pending[key]
			if !ok {
				continue
			}
			select {
			case ch <- &lookupResult{services: ack.GetServices()}:
			default:
			}
			delivered[key] = struct{}{}
		}
	}

	// 若 ACK 无 services，仍尝试唤醒所有 waiter，避免永久阻塞
	if len(ack.GetServices()) == 0 {
		for key, ch := range r.pending {
			select {
			case ch <- &lookupResult{services: nil}:
			default:
			}
			_ = key
		}
	}
}

func (r *rpcClient) lookup(serviceName, groupName string) ([]*gen.Service, error) {
	if r.cli == nil {
		return nil, errors.New("tcp client is nil")
	}
	if groupName == "" {
		groupName = r.groupName
	}

	node, err := strconv.Atoi(groupName)
	if err != nil {
		return nil, errors.New("invalid group name")
	}

	key := lookupKey(serviceName, groupName)
	ch := make(chan *lookupResult, 1)

	r.mu.Lock()
	if _, exists := r.pending[key]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("lookup already in progress: %s", key)
	}
	r.pending[key] = ch
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.pending, key)
		r.mu.Unlock()
	}()

	logger.Debugf("send lookup, serviceName=%s node=%d", serviceName, node)
	lookup := &gen.Lookup{
		ServiceName: serviceName,
		Node:        int32(node),
		Healthy:     true,
	}
	if err := r.cli.Write(uint8(gen.CMD_DISCOVERY),
		uint8(gen.SCMDDisco_LOOKUP),
		lookup); err != nil {
		logger.Errorf("lookup write failed: %v", err)
		return nil, err
	}

	timeout := r.timeout
	if timeout <= 0 {
		timeout = defaultLookupTimeout
	}

	select {
	case res := <-ch:
		if res == nil {
			return nil, errors.New("empty lookup result")
		}
		if res.err != nil {
			return nil, res.err
		}
		return res.services, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("lookup timeout after %s: %s", timeout, serviceName)
	}
}

func lookupKey(serviceName, groupName string) string {
	return serviceName + "@" + groupName
}

func pickInstance(svc *gen.Service) (*gen.Instance, error) {
	if svc == nil {
		return nil, errors.New("service is nil")
	}
	instances := svc.GetInstances()
	if len(instances) == 0 {
		return nil, fmt.Errorf("no instance in service %s", svc.GetName())
	}
	for _, inst := range instances {
		if inst != nil && inst.GetHealthy() && inst.GetEnable() {
			return inst, nil
		}
	}
	return instances[0], nil
}

func NewRPCDiscoveryClient(cli *network.TcpClient, redisCli *redis.Client, groupName string) (DiscoClient, error) {
	if cli == nil {
		return nil, errors.New("tcp client is nil")
	}
	if redisCli == nil {
		return nil, errors.New("redisCli is nil")
	}

	storage, err := cache.NewSharedCache(redisCli, ConfigCachePrefixKey)
	if err != nil {
		return nil, err
	}

	rc := &rpcClient{
		cli:       cli,
		redisCli:  redisCli,
		storage:   storage,
		groupName: groupName,
		timeout:   defaultLookupTimeout,
		pending:   make(map[string]chan *lookupResult),
	}

	// 自动包装现有 EventHandler，将 LOOKUP_ACK 回灌到 pending waiter
	cli.SetEventHandler(WrapEventHandler(cli.EventHandler(), rc))
	return rc, nil
}

// WrapEventHandler 将 LOOKUP_ACK 转发给 DiscoveryResponseHandler，其余事件交给 inner。
func WrapEventHandler(inner network.EventHandler, disco DiscoClient) network.EventHandler {
	handler, _ := disco.(DiscoveryResponseHandler)
	return &rpcEventHandler{inner: inner, lookup: handler}
}

type rpcEventHandler struct {
	inner  network.EventHandler
	lookup DiscoveryResponseHandler
}

func (h *rpcEventHandler) OnConnect(conn network.TCPConn) error {
	if h.inner == nil {
		return nil
	}
	return h.inner.OnConnect(conn)
}

func (h *rpcEventHandler) OnClose(conn network.TCPConn) {
	if h.inner != nil {
		h.inner.OnClose(conn)
	}
}

func (h *rpcEventHandler) OnCmdSystem(conn network.TCPConn, pkt proto.Message) error {
	if h.inner == nil {
		return nil
	}
	return h.inner.OnCmdSystem(conn, pkt)
}

func (h *rpcEventHandler) OnCmdDiscovery(conn network.TCPConn, pkt proto.Message) error {
	if ack, ok := pkt.(*gen.LookupAck); ok && h.lookup != nil {
		h.lookup.HandleLookupAck(ack)
	}
	if h.inner == nil {
		return nil
	}
	return h.inner.OnCmdDiscovery(conn, pkt)
}

func (h *rpcEventHandler) OnCmdDataReport(conn network.TCPConn, subcmd uint8, pkt proto.Message) error {
	if h.inner == nil {
		return nil
	}
	return h.inner.OnCmdDataReport(conn, subcmd, pkt)
}

func (h *rpcEventHandler) OnCmdConfig(conn network.TCPConn, pkt proto.Message) error {
	if h.inner == nil {
		return nil
	}
	return h.inner.OnCmdConfig(conn, pkt)
}

func (h *rpcEventHandler) OnCmdEvent(conn network.TCPConn, pkt proto.Message) error {
	if h.inner == nil {
		return nil
	}
	return h.inner.OnCmdEvent(conn, pkt)
}

func (h *rpcEventHandler) OnCmdControl(conn network.TCPConn, pkt proto.Message) error {
	if h.inner == nil {
		return nil
	}
	return h.inner.OnCmdControl(conn, pkt)
}
