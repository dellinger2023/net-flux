package naming

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type NacosConfigClient struct {
	client config_client.IConfigClient
}

func initConfigClient(cfg DiscoSetting) (config_client.IConfigClient, error) {
	serverConfigs := []constant.ServerConfig{
		{
			IpAddr: cfg.Host,
			Port:   uint64(cfg.Port),
		},
	}
	// 客户端配置
	clientConfig := constant.ClientConfig{
		NamespaceId:         cfg.Namespace, // 如果不需要命名空间，可以留空
		TimeoutMs:           5000,
		NotLoadCacheAtStart: !cfg.PreloadCache,
		LogDir:              cfg.LogDir,
		CacheDir:            cfg.CacheDir,
		LogLevel:            "info",
	}

	// 创建配置客户端
	configClient, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		return nil, err
	}
	return configClient, nil
}

func (c *NacosConfigClient) SetConfig(dataId, group, content string) error {
	_, err := c.client.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   group,
		Content: content,
	})
	return err
}

func (c *NacosConfigClient) GetConfig(dataId, group string) (string, error) {

	return c.client.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
}

func (c *NacosConfigClient) DeleteConfig(dataId, group string) error {
	_, err := c.client.DeleteConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	return err
}

func (c *NacosConfigClient) ListenConfig(dataId, group string, onChange func(namespace, group, dataId, data string)) error {
	return c.client.ListenConfig(vo.ConfigParam{
		DataId:   dataId,
		Group:    group,
		OnChange: onChange,
	})
}

func (c *NacosConfigClient) CancelListenConfig(dataId, group string) error {
	return c.client.CancelListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
}

func (c *NacosConfigClient) SearchConfig(search, group, dataId string) (*model.ConfigPage, error) {

	return c.client.SearchConfig(vo.SearchConfigParam{
		Search: search,
		Group:  group,
		DataId: dataId,
	})
}

func NewConfigClient(cfg DiscoSetting) (*NacosConfigClient, error) {
	client, err := initConfigClient(cfg)
	if err != nil {
		return nil, err
	}
	return &NacosConfigClient{client: client}, nil
}
