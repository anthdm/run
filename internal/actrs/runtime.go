package actrs

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/raptor/internal/runtime"
	"github.com/anthdm/raptor/internal/shared"
	"github.com/anthdm/raptor/internal/storage"
	"github.com/anthdm/raptor/internal/types"
	"github.com/anthdm/raptor/proto"
	"github.com/google/uuid"
	"github.com/tetratelabs/wazero"

	prot "google.golang.org/protobuf/proto"
)

const KindRuntime = "runtime"

var (
	runtimeKeepAlive = time.Second
)

type shutdown struct{}

// Runtime is an actor that can execute compiled WASM blobs in a distributed cluster.
type Runtime struct {
	store      storage.Store
	cache      storage.ModCacher
	started    time.Time
	deployment *types.Deployment
	managerPID *actor.PID
	runtime    *runtime.Runtime
	repeat     actor.SendRepeater
	stdout     *bytes.Buffer
}

func NewRuntime(store storage.Store, cache storage.ModCacher) actor.Producer {
	return func() actor.Receiver {
		return &Runtime{
			store:  store,
			cache:  cache,
			stdout: &bytes.Buffer{},
		}
	}
}

func (r *Runtime) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Started:
		r.started = time.Now()
		r.repeat = c.SendRepeat(c.PID(), shutdown{}, runtimeKeepAlive)
		r.managerPID = c.Engine().Registry.GetPID(KindRuntimeManager, "1")
	case actor.Stopped:
		// TODO: send metrics about the runtime to the metric actor.
		_ = time.Since(r.started)
		c.Send(r.managerPID, removeRuntime{key: r.deployment.ID.String()})
		r.runtime.Close()
		// Releasing this mod will invalidate the cache for some reason.
		// r.mod.Close(context.TODO())
	case *proto.HTTPRequest:
		// Refresh the keepAlive timer
		r.repeat = c.SendRepeat(c.PID(), shutdown{}, runtimeKeepAlive)
		if r.runtime == nil {
			r.initialize(msg)
		}
		// Handle the HTTP request that is forwarded from the WASM server actor.
		r.handleHTTPRequest(c, msg)
	case shutdown:
		c.Engine().Poison(c.PID())
	}
}

func (r *Runtime) initialize(msg *proto.HTTPRequest) error {
	id := uuid.MustParse(msg.DeploymentID)
	// TODO: this could be coming from a Redis cache instead of Postres.
	// Maybe only the blob. Not sure...
	deploy, err := r.store.GetDeployment(id)
	if err != nil {
		return fmt.Errorf("runtime: could not find deployment (%s)", id)
	}
	r.deployment = deploy

	modCache, ok := r.cache.Get(r.deployment.ID)
	if !ok {
		slog.Warn("no cache hit", "endpoint", r.deployment.ID)
		modCache = wazero.NewCompilationCache()
	}

	args := runtime.Args{
		Cache:        modCache,
		DeploymentID: deploy.ID,
		Blob:         deploy.Blob,
		Engine:       msg.Runtime,
		Stdout:       r.stdout,
	}
	run, err := runtime.New(context.Background(), args)
	if err != nil {
		return err
	}
	r.runtime = run
	r.cache.Put(deploy.ID, modCache)

	return nil
}

func (r *Runtime) handleHTTPRequest(ctx *actor.Context, msg *proto.HTTPRequest) {
	start := time.Now()
	b, err := prot.Marshal(msg)
	if err != nil {
		slog.Warn("failed to marshal incoming HTTP request", "err", err)
		respondError(ctx, http.StatusInternalServerError, "internal server error", msg.ID)
		return
	}

	var args []string = nil
	if msg.Runtime == "js" {

		args = []string{"", "-e", string(r.deployment.Blob)}
	}

	req := bytes.NewReader(b)
	if err := r.runtime.Invoke(req, msg.Env, args...); err != nil {
		slog.Warn("runtime invoke error", "err", err)
		respondError(ctx, http.StatusInternalServerError, "internal server error", msg.ID)
		return
	}

	res, status, err := shared.ParseRuntimeHTTPResponse(r.stdout.String())
	if err != nil {
		respondError(ctx, http.StatusInternalServerError, "invalid response", msg.ID)
		return
	}
	resp := &proto.HTTPResponse{
		Response:   []byte(res),
		RequestID:  msg.ID,
		StatusCode: int32(status),
	}

	ctx.Respond(resp)
	r.stdout.Reset()

	// only send metrics when its a request on LIVE
	if !msg.Preview {
		metric := types.RequestMetric{
			ID:           uuid.New(),
			Duration:     time.Since(start),
			DeploymentID: r.deployment.ID,
			// EndpointID:   deploy.EndpointID,
			RequestURL: msg.URL,
			StatusCode: status,
		}
		pid := ctx.Engine().Registry.GetPID(KindMetric, "1")
		ctx.Send(pid, metric)
	}
}

func respondError(ctx *actor.Context, code int32, msg string, id string) {
	ctx.Respond(&proto.HTTPResponse{
		Response:   []byte(msg),
		StatusCode: code,
		RequestID:  id,
	})
}
