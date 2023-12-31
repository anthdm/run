package actrs

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/anthdm/ffaas/pkg/storage"
	"github.com/anthdm/ffaas/proto"
	"github.com/anthdm/hollywood/actor"
	"github.com/google/uuid"
	"github.com/stealthrocket/wasi-go"
	"github.com/stealthrocket/wasi-go/imports"
	"github.com/tetratelabs/wazero"
)

const KindRuntime = "runtime"

// Runtime is an actor that can execute compiled WASM blobs in a distributed cluster.
type Runtime struct {
	store storage.Store
}

func NewRuntime(store storage.Store) actor.Producer {
	return func() actor.Receiver {
		return &Runtime{
			store: store,
		}
	}
}

func (r *Runtime) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Started:
	case actor.Stopped:
	case *proto.HTTPRequest:
		endpoint, err := r.store.GetEndpoint(uuid.MustParse(msg.EndpointID))
		if err != nil {
			slog.Warn("runtime could not find endpoint from store", "err", err)
			return
		}
		deploy, err := r.store.GetDeploy(endpoint.ActiveDeployID)
		if err != nil {
			slog.Warn("runtime could not find the endpoint's active deploy from store", "err", err)
			return
		}
		cache := wazero.NewCompilationCache()
		r.exec(context.TODO(), deploy.Blob, cache, endpoint.Environment)
		c.Respond(&proto.HTTPResponse{Response: []byte("hello")})
	}
}

func (r *Runtime) exec(ctx context.Context, blob []byte, cache wazero.CompilationCache, env map[string]string) {
	config := wazero.NewRuntimeConfig().WithCompilationCache(cache)
	runtime := wazero.NewRuntimeWithConfig(ctx, config)
	defer runtime.Close(ctx)

	mod, err := runtime.CompileModule(ctx, blob)
	if err != nil {
		slog.Warn("compiling module failed", "err", err)
		return
	}
	fd := -1
	builder := imports.NewBuilder().
		WithName("ffaas").
		WithArgs().
		WithStdio(fd, fd, fd).
		WithEnv(envMapToSlice(env)...).
		// TODO: we want to mount this to some virtual folder?
		WithDirs("/").
		WithListens().
		WithDials().
		WithNonBlockingStdio(false).
		WithSocketsExtension("auto", mod).
		WithMaxOpenFiles(10).
		WithMaxOpenDirs(10)

	var system wasi.System
	ctx, system, err = builder.Instantiate(ctx, runtime)
	if err != nil {
		slog.Warn("failed to instanciate wasi module", "err", err)
		return
	}
	defer system.Close(ctx)

	_, err = runtime.InstantiateModule(ctx, mod, wazero.NewModuleConfig())
	if err != nil {
		slog.Warn("failed to instanciate guest module", "err", err)
	}
}

func envMapToSlice(env map[string]string) []string {
	slice := make([]string, len(env))
	i := 0
	for k, v := range env {
		s := fmt.Sprintf("%s=%s", k, v)
		slice[i] = s
		i++
	}
	return slice
}