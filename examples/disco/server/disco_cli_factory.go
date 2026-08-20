package main

import (
	"fmt"
	"sync"

	"github.com/dellinger2023/net-flux/pkg/naming"
	"github.com/dellinger2023/net-flux/pkg/util"
)

type storageItem map[string]naming.DiscoClient

type discoClientFactory struct {
	sync.RWMutex
	settings       *naming.DiscoSetting
	serviceStorage map[int]storageItem
}

func newDiscoClientFactory(settings *naming.DiscoSetting) *discoClientFactory {
	return &discoClientFactory{
		settings:       settings,
		serviceStorage: make(map[int]storageItem),
	}
}

func (s *discoClientFactory) GetDiscoClient(connId int, serviceName string) naming.DiscoClient {
	if util.IsEmptyStr(serviceName) {
		serviceName = "web"
	}

	s.RLock()
	if item, ok := s.serviceStorage[connId]; ok {
		if cli, ok := item[serviceName]; ok {
			s.RUnlock()
			return cli
		}
	}
	s.RUnlock()

	cli, err := naming.NewNacosDiscoverClient(*s.settings)
	if err != nil {
		fmt.Println("create disco client failed: ", err)
		return nil
	}

	s.Lock()
	defer s.Unlock()

	item, ok := s.serviceStorage[connId]
	if !ok {
		item = make(storageItem)
		s.serviceStorage[connId] = item
	}
	if existing, ok := item[serviceName]; ok {
		cli.Close()
		return existing
	}
	item[serviceName] = cli
	return cli
}

func (s *discoClientFactory) RemoveDiscoClient(connId int, serviceName string) {
	s.Lock()
	defer s.Unlock()
	if clients, exists := s.serviceStorage[connId]; exists {
		if cli, found := clients[serviceName]; found {
			cli.Close()
		}
		delete(s.serviceStorage[connId], serviceName)
	}
}

func (s *discoClientFactory) RemoveAll(connId int) {
	s.Lock()
	defer s.Unlock()
	if clients, exists := s.serviceStorage[connId]; exists {
		for _, c := range clients {
			c.Close()
		}
	}
	delete(s.serviceStorage, connId)
}
