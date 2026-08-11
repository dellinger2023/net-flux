package naming

import "github.com/dellinger2023/net-flux/gen"

type DiscoSetting struct {
	Host         string `mapstructure:"host" json:"host" yaml:"host"`
	Port         int    `mapstructure:"port" json:"port" yaml:"port"`
	Namespace    string `mapstructure:"namespace" json:"namespace" yaml:"namespace"`
	LogDir       string `mapstructure:"log_dir" json:"log_dir" yaml:"log_dir"`
	CacheDir     string `mapstructure:"cache_dir" json:"cache_dir" yaml:"cache_dir"`
	PreloadCache bool   `mapstructure:"preload_cache" json:"preload_cache" yaml:"preload_cache"`
	Timeout      int    `mapstructure:"timeout" json:"timeout" yaml:"timeout"`
	GroupName    string `mapstructure:"group" json:"group" yaml:"group"`
	Username     string `mapstructure:"username" json:"username" yaml:"username"`
	Password     string `mapstructure:"password" json:"password" yaml:"password"`
	Node         int    `mapstructure:"node" json:"node" yaml:"node"`
}

type ConfigItem struct {
	Id      string `param:"id"`
	DataId  string `param:"dataId"`
	Group   string `param:"group"`
	Content string `param:"content"`
	Md5     string `param:"md5"`
	Tenant  string `param:"tenant"`
	Appname string `param:"appname"`
}

type ConfigPage struct {
	TotalCount     int          `param:"totalCount"`
	PageNumber     int          `param:"pageNumber"`
	PagesAvailable int          `param:"pagesAvailable"`
	PageItems      []ConfigItem `param:"pageItems"`
}

type DiscoClient interface {
	GetGroupName() string
	RegisterInstance(instance *gen.Instance) error
	DeregisterInstance(serviceName, groupName, ip string, port uint64) error
	GetAllServices(groupName string) ([]string, error)
	GetService(serviceName, groupName string, clusters []string) (*gen.Service, error)
	GetServiceInstanceByName(serviceName string) (*gen.Instance, error)
	GetServiceInstance(serviceName, groupName string, clusters []string) (*gen.Instance, error)
	GetServiceInstanceByGroup(serviceName, groupName string) (*gen.Instance, error)
	GetServiceInstancesByName(serviceName string) ([]*gen.Instance, error)
	GetServiceInstances(serviceName, groupName string, clusters []string) ([]*gen.Instance, error)

	SetConfig(dataId, content string) error
	GetConfig(dataId string) (string, error)
	DeleteConfig(dataId string) error
	ListenConfig(dataId string, onChange func(namespace, group, dataId, data string)) error
	CancelListenConfig(dataId string) error
	SearchConfig(search, dataId string) (*ConfigPage, error)

	Close()
}
