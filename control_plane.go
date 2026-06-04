package apollo

import (
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/selector"
	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx/log"
)

// ControlPlaneCapabilities declares Apollo's explicit control plane contract.
func (p *PlugApollo) ControlPlaneCapabilities() []lynx.ControlPlaneCapability {
	return []lynx.ControlPlaneCapability{
		lynx.ControlPlaneCapabilityConfig,
		lynx.ControlPlaneCapabilityWatcher,
	}
}

// HTTPRateLimit returns nil: Apollo is a config center and provides no rate limiting.
func (p *PlugApollo) HTTPRateLimit() middleware.Middleware {
	log.Debugf("Apollo plugin does not support HTTP rate limiting, returning nil")
	return nil
}

// GRPCRateLimit returns nil: Apollo is a config center and provides no rate limiting.
func (p *PlugApollo) GRPCRateLimit() middleware.Middleware {
	log.Debugf("Apollo plugin does not support gRPC rate limiting, returning nil")
	return nil
}

// NewServiceRegistry returns nil: Apollo is a config center, not a service registry.
func (p *PlugApollo) NewServiceRegistry() registry.Registrar {
	log.Debugf("Apollo plugin does not support service registration, returning nil")
	return nil
}

// NewServiceDiscovery returns nil: Apollo is a config center, not a discovery service.
func (p *PlugApollo) NewServiceDiscovery() registry.Discovery {
	log.Debugf("Apollo plugin does not support service discovery, returning nil")
	return nil
}

// NewNodeRouter returns nil: Apollo is a config center and does no service routing.
func (p *PlugApollo) NewNodeRouter(serviceName string) selector.NodeFilter {
	log.Debugf("Apollo plugin does not support service routing, returning nil for service: %s", serviceName)
	return nil
}
