package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Run starts the runtime listeners and shuts them down when ctx ends or a
// listener fails. Call Close after Run returns.
func (r *Runtime) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("runtime must not be nil")
	}
	if len(r.servers) == 0 {
		return errors.New("runtime has no listeners")
	}

	servers := r.servers
	errCh := make(chan error, len(servers))
	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(srv *http.Server) {
			defer wg.Done()
			err := srv.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("listen on %s: %w", srv.Addr, err)
			}
		}(srv)
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, srv := range servers {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
			if runErr == nil {
				runErr = fmt.Errorf("shutdown %s: %w", srv.Addr, err)
			}
		}
	}
	wg.Wait()

	if runErr == nil {
		select {
		case runErr = <-errCh:
		default:
		}
	}
	return runErr
}
