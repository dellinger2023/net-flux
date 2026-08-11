package naming

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

const (
	DefaultGroupName = "DEFAULT_GROUP"
)

type NacosNamingClient struct {
	client naming_client.INamingClient
}

func initNamingClient(cfg DiscoSetting) (naming_client.INamingClient, error) {
	serverConfigs := []constant.ServerConfig{
		{
			IpAddr: cfg.Host,
			Port:   uint64(cfg.Port),
		},
	}
	// 客户端配置：NamespaceId 必须填 Nacos 控制台「命名空间」里的「命名空间 ID」（不是显示名称）
	clientConfig := constant.ClientConfig{
		NamespaceId:         cfg.Namespace, // 留空或 "public" 表示默认命名空间
		TimeoutMs:           5000,
		NotLoadCacheAtStart: !cfg.PreloadCache,
		LogDir:              cfg.LogDir,
		CacheDir:            cfg.CacheDir,
		LogLevel:            "info",
		Username:            cfg.Username,
		Password:            cfg.Password,
	}

	// 创建命名客户端
	namingClient, err := clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		return nil, err
	}
	return namingClient, nil
}

func (c *NacosNamingClient) RegisterInstance(serviceName, groupName, ip string,
	port uint64, metadata map[string]string) error {
	_, err := c.client.RegisterInstance(vo.RegisterInstanceParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Ip:          ip,
		Port:        uint64(port),
		Enable:      true,
		Healthy:     true,
		Weight:      1.0,
		Ephemeral:   true, // 临时实例，便于在控制台显示并通过心跳保活
		Metadata:    metadata,
	})
	return err
}

func (c *NacosNamingClient) DeregisterInstance(serviceName, groupName, ip string, port uint64) error {
	_, err := c.client.DeregisterInstance(vo.DeregisterInstanceParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Ip:          ip,
		Port:        uint64(port),
	})

	return err
}

func (c *NacosNamingClient) GetAllServices(groupName string) ([]string, error) {

	services, err := c.client.GetAllServicesInfo(vo.GetAllServiceInfoParam{
		GroupName: groupName,
	})
	if err != nil {
		return nil, err
	}
	return services.Doms, nil
}

func (c *NacosNamingClient) GetService(serviceName, groupName string, clusters []string) (model.Service, error) {
	return c.client.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Clusters:    clusters,
	})
}

func (c *NacosNamingClient) GetServiceInstanceByName(serviceName string) (*model.Instance, error) {
	return c.client.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: serviceName,
	})
}

func (c *NacosNamingClient) GetServiceInstance(serviceName, groupName string, clusters []string) (*model.Instance, error) {
	return c.client.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Clusters:    clusters,
	})
}

func (c *NacosNamingClient) GetServiceInstancesByName(serviceName string) ([]model.Instance, error) {
	return c.GetServiceInstances(serviceName, DefaultGroupName, nil)
}

func (c *NacosNamingClient) GetServiceInstances(serviceName, groupName string, clusters []string) ([]model.Instance, error) {
	return c.client.SelectAllInstances(vo.SelectAllInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Clusters:    clusters,
	})
}

func NewNamingClient(cfg DiscoSetting) (*NacosNamingClient, error) {
	client, err := initNamingClient(cfg)
	if err != nil {
		return nil, err
	}
	return &NacosNamingClient{client: client}, nil
}
