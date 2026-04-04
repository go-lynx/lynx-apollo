package apollo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx/log"
)

// ApolloConfigWatcher implements config.Watcher interface for Apollo
type ApolloConfigWatcher struct {
	client              *ApolloHTTPClient
	namespace           string
	stopCh              chan struct{}
	notifyCh            chan []*config.KeyValue
	mu                  sync.RWMutex
	notificationId      int64
	watchCtx            context.Context
	cancelWatch         context.CancelFunc
	notificationTimeout time.Duration
}

// NewApolloConfigWatcher creates a new Apollo config watcher
func NewApolloConfigWatcher(client *ApolloHTTPClient, namespace string) *ApolloConfigWatcher {
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	return &ApolloConfigWatcher{
		client:              client,
		namespace:           namespace,
		stopCh:              make(chan struct{}),
		notifyCh:            make(chan []*config.KeyValue, 1),
		watchCtx:            watchCtx,
		cancelWatch:         cancelWatch,
		notificationTimeout: 60 * time.Second,
	}
}

// Next returns the next configuration change
func (w *ApolloConfigWatcher) Next() ([]*config.KeyValue, error) {
	select {
	case kvs := <-w.notifyCh:
		return kvs, nil
	case <-w.stopCh:
		return nil, fmt.Errorf("watcher stopped")
	}
}

// Stop stops the watcher
func (w *ApolloConfigWatcher) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.stopCh:
		// Already stopped
		return nil
	default:
		if w.cancelWatch != nil {
			w.cancelWatch()
		}
		close(w.stopCh)
		return nil
	}
}

// Start starts watching for configuration changes
func (w *ApolloConfigWatcher) Start() {
	go w.watchLoop()
}

// watchLoop continuously watches for configuration changes
func (w *ApolloConfigWatcher) watchLoop() {
	for {
		select {
		case <-w.stopCh:
			log.Infof("Apollo config watcher stopped for namespace: %s", w.namespace)
			return
		default:
			// Watch for notifications using long polling
			notifications, err := w.client.WatchNotifications(w.watchCtx, w.namespace, w.notificationId, w.notificationTimeout)
			if err != nil {
				if errors.Is(w.watchCtx.Err(), context.Canceled) {
					log.Infof("Apollo config watcher canceled for namespace: %s", w.namespace)
					return
				}
				log.Errorf("Failed to watch Apollo notifications for namespace %s: %v", w.namespace, err)
				// Wait a bit before retrying
				select {
				case <-w.stopCh:
					return
				case <-time.After(5 * time.Second):
					continue
				}
			}

			// Process notifications
			if len(notifications) > 0 {
				for _, notification := range notifications {
					if notification.NamespaceName == w.namespace {
						// Update notification ID
						w.mu.Lock()
						w.notificationId = notification.NotificationId
						w.mu.Unlock()

						// Reload configuration
						configResp, err := w.client.GetConfig(w.watchCtx, w.namespace)
						if err != nil {
							if errors.Is(w.watchCtx.Err(), context.Canceled) {
								return
							}
							log.Errorf("Failed to reload config after notification: %v", err)
							continue
						}

						// Convert to KeyValue list
						var kvs []*config.KeyValue
						for key, value := range configResp.Configurations {
							kvs = append(kvs, &config.KeyValue{
								Key:   key,
								Value: []byte(value),
							})
						}

						// Send notification
						select {
						case w.notifyCh <- kvs:
						case <-w.stopCh:
							return
						}
					}
				}
			}
		}
	}
}
