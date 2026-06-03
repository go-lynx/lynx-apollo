package apollo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	startOnce           sync.Once
	stopOnce            sync.Once
	wg                  sync.WaitGroup
	closed              int32
}

// NewApolloConfigWatcher creates a new Apollo config watcher.
// The watcher's internal context is derived from context.Background() at
// construction time.  If a lifecycle context is available, callers should
// use StartWithContext instead of Start so that plugin shutdown propagates
// to the in-flight long-poll request.
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
	if atomic.LoadInt32(&w.closed) == 1 {
		return nil, fmt.Errorf("watcher stopped")
	}
	select {
	case kvs, ok := <-w.notifyCh:
		if !ok {
			return nil, fmt.Errorf("watcher stopped")
		}
		return kvs, nil
	case <-w.stopCh:
		return nil, fmt.Errorf("watcher stopped")
	}
}

// Stop stops the watcher
func (w *ApolloConfigWatcher) Stop() error {
	w.stopOnce.Do(func() {
		atomic.StoreInt32(&w.closed, 1)
		if w.cancelWatch != nil {
			w.cancelWatch()
		}
		close(w.stopCh)
	})
	w.wg.Wait()
	return nil
}

// Start starts the watcher, deriving the internal watch context from ctx.
// This implements the Start(context.Context) error interface recognised by
// lynx.StartControlPlaneWatcher, so that lifecycle cancellation is
// propagated directly into in-flight long-poll HTTP requests.
//
// Callers that do not have a lifecycle context available may pass
// context.Background().
func (w *ApolloConfigWatcher) Start(ctx context.Context) error {
	// Replace the default background-derived watchCtx with one tied to ctx
	// so WatchNotifications can be cancelled by the caller.
	newCtx, newCancel := context.WithCancel(ctx)
	w.mu.Lock()
	if w.cancelWatch != nil {
		w.cancelWatch()
	}
	w.watchCtx = newCtx
	w.cancelWatch = newCancel
	w.mu.Unlock()

	w.startOnce.Do(func() {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.watchLoop()
		}()
	})
	return nil
}

// watchLoop continuously watches for configuration changes using Apollo's
// long-polling notification API.  It runs until stopCh is closed or the
// internal watchCtx is cancelled.  A top-level panic recovery prevents an
// unexpected panic from silently killing the goroutine.
func (w *ApolloConfigWatcher) watchLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Unexpected panic in Apollo watchLoop for namespace %s: %v", w.namespace, r)
		}
	}()

	for {
		select {
		case <-w.stopCh:
			log.Infof("Apollo config watcher stopped for namespace: %s", w.namespace)
			return
		default:
			// Snapshot watchCtx under read-lock so we use the most recent context.
			w.mu.RLock()
			watchCtx := w.watchCtx
			notificationId := w.notificationId
			w.mu.RUnlock()

			// Watch for notifications using long polling
			notifications, err := w.client.WatchNotifications(watchCtx, w.namespace, notificationId, w.notificationTimeout)
			if err != nil {
				if errors.Is(watchCtx.Err(), context.Canceled) {
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
						configResp, err := w.client.GetConfig(watchCtx, w.namespace)
						if err != nil {
							if errors.Is(watchCtx.Err(), context.Canceled) {
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
						case <-w.stopCh:
							return
						case w.notifyCh <- kvs:
						}
					}
				}
			}
		}
	}
}
