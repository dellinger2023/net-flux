package balancer

import (
	"github.com/dellinger2023/net-flux/gen"
)

/**
 * Balancer is an interface that represents a balancer.
 * It is used to pick a instance from a service.
 * It is used to resolve a stream to a instance.
 */
type Balancer interface {
	// Pick picks an instance from a service.
	Pick(service *gen.Service) (*gen.Instance, error)

	// Resolve resolves a stream to an instance.
	Resolve(serviceName, streamId string) (*gen.Instance, error)
}
