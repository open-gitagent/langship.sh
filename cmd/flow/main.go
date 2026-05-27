package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lyzrai/flow/pkg/api"
	"github.com/lyzrai/flow/pkg/engine"
	"github.com/lyzrai/flow/pkg/execevents"
	"github.com/lyzrai/flow/pkg/executors"
	"github.com/lyzrai/flow/pkg/logstore"
	"github.com/lyzrai/flow/pkg/orchestrator"
	"github.com/lyzrai/flow/pkg/secrets"
	"github.com/lyzrai/flow/pkg/storage"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "run":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: flow run <workflow.json>")
			os.Exit(2)
		}
		os.Exit(runWorkflow(os.Args[2]))
	case "serve":
		os.Exit(serve())
	case "version":
		fmt.Println("flow v0.0.1-dev")
	default:
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `flow — durable n8n-compatible workflow engine

usage:
  flow run <workflow.json>   execute a workflow file once and print outputs
  flow serve                 start the HTTP server (UI + API) on :8080
  flow version               print version

env vars (for `+"`flow serve`"+`):
  FLOW_ADDR                  HTTP listen address (default :8090)
  FLOW_CORS_ORIGINS          comma-separated allow-list (default *)
  FLOW_PUBLIC_URL            externally-reachable base URL (used for webhook callbacks)
  MONGO_URI                  Mongo connection string (required)
  MONGO_DB                   Mongo database name (default flow)
  RESTATE_INGRESS_URL        Restate ingress URL (default http://localhost:8081)
  RESTATE_ADMIN_URL          Restate admin URL (default http://localhost:9070)
  RESTATE_SERVICE_ADDR       Restate service-endpoint listen addr (default :9080)
  RESTATE_DEPLOYMENT_URI     How Restate reaches this service (default http://host.docker.internal:9080)
  BUILDKIT_HOST              BuildKit gRPC address (default tcp://127.0.0.1:1234)`)
}

func runWorkflow(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("failed to read workflow file", slog.String("path", path), slog.Any("error", err))
		return 1
	}

	wf, err := engine.ParseWorkflow(data)
	if err != nil {
		slog.Error("failed to parse workflow", slog.Any("error", err))
		return 1
	}

	executors.RegisterAll()

	ctx := context.Background()
	result, err := engine.RunWorkflow(ctx, wf, nil, executors.BuildLookup())
	if err != nil {
		slog.Error("workflow run failed", slog.Any("error", err))
		return 1
	}

	out, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		slog.Error("failed to marshal result", slog.Any("error", err))
		return 1
	}
	fmt.Println(string(out))
	return 0
}

func serve() int {
	// We register executors after Mongo is up so the Build executor can
	// look up agents from storage. See below.
	var lookup engine.ExecutorLookup

	addr := envOr("FLOW_ADDR", ":8090")
	ingressURL := envOr("RESTATE_INGRESS_URL", "http://localhost:8081")
	adminURL := envOr("RESTATE_ADMIN_URL", "http://localhost:9070")
	serviceAddr := envOr("RESTATE_SERVICE_ADDR", ":9080")
	// Default to host.docker.internal so Restate-in-Docker can reach the
	// flow service when `make watch` runs on the host (macOS/Windows). On
	// Linux without docker-desktop's host-gateway alias you'll need to set
	// RESTATE_DEPLOYMENT_URI yourself. In docker-compose, the flow service
	// overrides this via env to `http://flow:9080`.
	deployURI := envOr("RESTATE_DEPLOYMENT_URI", "http://host.docker.internal"+serviceAddr)
	mongoURI := envOr("MONGO_URI", "")
	mongoDB := envOr("MONGO_DB", "flow")

	// Mongo is a hard dependency — pipelines/runs/agents all live there.
	mongoCtx, mongoCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer mongoCancel()
	mongo, err := storage.NewMongo(mongoCtx, mongoURI, mongoDB)
	if err != nil {
		slog.Error("mongo not reachable, exiting",
			slog.String("hint", "set MONGO_URI (e.g. mongodb://localhost:27017)"),
			slog.Any("error", err),
		)
		return 1
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mongo.Close(closeCtx)
	}()
	slog.Info("mongo connected", slog.String("db", mongoDB))

	// FLOW_SECRET_KEY is required for credential encryption. Check early so
	// the failure is explicit rather than surfacing per-request.
	// Note: This is a breaking change for deployments that predate this PR.
	// If upgrading from a version without secrets.IsConfigured() checks, operators
	// must set FLOW_SECRET_KEY before deploying. Existing deployments with unsealed
	// agents will continue to work (GetPAT falls back to plaintext), but new agents
	// and credential writes require the key to be set.
	if !secrets.IsConfigured() {
		slog.Error("FLOW_SECRET_KEY is not set, exiting",
			slog.String("hint", "set FLOW_SECRET_KEY to any non-empty value for encryption of PAT tokens, AWS keys, GCP service accounts, and KV secrets"),
		)
		return 1
	}

	// MinIO is optional — when MINIO_ENDPOINT is unset we skip log
	// archiving. Live SSE log streaming still works regardless.
	var logs logstore.Store
	if endpoint := envOr("MINIO_ENDPOINT", ""); endpoint != "" {
		minioCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		m, err := logstore.NewMinio(minioCtx, logstore.Config{
			Endpoint:  endpoint,
			AccessKey: envOr("MINIO_ACCESS_KEY", "minio"),
			SecretKey: envOr("MINIO_SECRET_KEY", "minio12345"),
			Bucket:    envOr("MINIO_BUCKET", "flow-logs"),
			UseSSL:    envOr("MINIO_USE_SSL", "false") == "true",
		})
		cancel()
		if err != nil {
			slog.Warn("minio init failed, log archive disabled",
				slog.String("endpoint", endpoint),
				slog.Any("error", err),
			)
		} else {
			logs = m
			slog.Info("minio connected",
				slog.String("endpoint", endpoint),
				slog.String("bucket", envOr("MINIO_BUCKET", "flow-logs")),
			)
		}
	}

	// Register executors now that storage is ready; Build reads from
	// AgentStore for clone targets, Deploy looks up cloud credentials
	// (agent-scoped first, global fallback).
	executors.RegisterAll(executors.RegistryDeps{
		Agents:      mongo.Agents(),
		Credentials: mongo.Credentials(),
	})
	lookup = executors.BuildLookup()

	slog.Info("checking restate",
		slog.String("ingress", ingressURL),
		slog.String("admin", adminURL),
	)
	if err := orchestrator.HealthCheckRestate(ingressURL); err != nil {
		slog.Error("restate not reachable, exiting",
			slog.String("hint", "run `docker compose up restate` (or set RESTATE_INGRESS_URL=)"),
			slog.Any("error", err),
		)
		return 1
	}

	orch := orchestrator.NewRestateOrchestrator(ingressURL)

	// In-memory event bus shared between the orchestrator (publisher) and
	// the API server (SSE subscribers). One bus per process is fine for
	// the self-hosted single-process topology.
	eventBus := execevents.NewMemoryBus()

	// Start the Restate service endpoint that Restate calls back into.
	go startRestateService(serviceAddr, lookup, mongo.Runs(), eventBus)

	// Auto-register with Restate admin so callbacks land on us.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := orchestrator.RegisterDeploymentWithRetry(ctx, adminURL, deployURI, 30, 2*time.Second); err != nil {
			slog.Error("restate deployment registration failed",
				slog.String("admin", adminURL),
				slog.String("deploy_uri", deployURI),
				slog.Any("error", err),
			)
			return
		}
		slog.Info("registered with restate", slog.String("deploy_uri", deployURI))
	}()

	// HTTP API server. The UI runs in a separate process (nginx in prod,
	// `next dev` locally) and proxies /api to here.
	srv := &http.Server{
		Addr: addr,
		Handler: api.NewServer(api.ServerDeps{
			Orchestrator:      orch,
			RestateIngressURL: orch.IngressURL(),
			CORSOrigins:       parseCSV(envOr("FLOW_CORS_ORIGINS", "*")),
			PublicURL:         envOr("FLOW_PUBLIC_URL", ""),
			Pipelines:         mongo.Pipelines(),
			Runs:              mongo.Runs(),
			Agents:            mongo.Agents(),
			Credentials:       mongo.Credentials(),
			Environments:      mongo.Environments(),
			Events:            eventBus,
			Logs:              logs,
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("flow listening", slog.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("forced shutdown", slog.Any("error", err))
		return 1
	}
	return 0
}

func startRestateService(addr string, lookup engine.ExecutorLookup, runs orchestrator.ExecutionCompleter, emitter engine.Emitter) {
	rs := orchestrator.NewRestateServer(lookup, nil, runs, emitter)
	slog.Info("starting restate service endpoint", slog.String("addr", addr))
	if err := rs.Start(context.Background(), addr); err != nil {
		slog.Error("restate service failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseCSV(v string) []string {
	out := []string{}
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
