package apollo

import (
	"context"
	"testing"
	"time"

	"github.com/go-lynx/lynx-apollo/conf"
	"github.com/go-lynx/lynx/pkg/security"
	"github.com/go-lynx/lynx/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The production lifecycle policy (lynx/internal/app/lifecycle_policy.go) rejects
// any plugin for which plugins.HasTrueContextLifecycle is false when
// security.IsProduction() is true. These tests pin the plugin to that contract.
func TestPlugApollo_HasTrueContextLifecycle(t *testing.T) {
	p := NewApolloConfigCenter()

	caps := plugins.DescribePluginCapabilities(p)
	assert.True(t, caps.HasLifecycleWithCtx, "plugin must expose StartContext/StopContext/InitializeContext")
	assert.True(t, caps.HasContextSteps, "plugin must implement a context-aware step hook")
	assert.True(t, caps.IsTrulyContextAware)
	assert.True(t, plugins.HasTrueContextLifecycle(p))

	_, ok := plugins.GetTrueContextLifecycle(p)
	assert.True(t, ok)

	var _ plugins.ContextStartupTasker = p
	var _ plugins.ContextCleanupTasker = p
}

func TestPlugApollo_ProductionLifecyclePolicyAccepts(t *testing.T) {
	t.Setenv("LYNX_ENV", "production")
	require.True(t, security.IsProduction())

	p := NewApolloConfigCenter()
	assert.True(t, plugins.HasTrueContextLifecycle(p),
		"plugin %s would be rejected by the production lifecycle policy", p.Name())
}

func TestPlugApollo_StartupTasksContext_ObservesCancellation(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{
		AppId:      "test-app",
		MetaServer: "http://127.0.0.1:1",
		Cluster:    conf.DefaultCluster,
		Namespace:  conf.DefaultNamespace,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := p.StartupTasksContext(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second, "cancelled startup must return promptly")
	assert.Nil(t, p.client, "no client must be left behind after a cancelled startup")
	assert.False(t, p.IsInitialized())
}

func TestPlugApollo_CleanupTasksContext_ReportsCancellation(t *testing.T) {
	p := NewApolloConfigCenter()
	p.conf = &conf.Apollo{AppId: "test-app", MetaServer: "http://127.0.0.1:1"}
	p.client = NewApolloHTTPClient("http://127.0.0.1:1", "test-app", conf.DefaultCluster, conf.DefaultNamespace, "", time.Second)
	p.setInitialized()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.CleanupTasksContext(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, p.IsDestroyed(), "cleanup still releases local state")
	assert.Nil(t, p.client)
}
