<div align="center">

# Langship

**Any framework. Any runtime.**

Open-source, self-hosted **deployment · governance · operations** for agent apps.
One pipeline definition → Kubernetes, AWS Bedrock AgentCore, or Vertex AI Agent
Engine — same governance everywhere. Works with LangGraph, LangChain, LlamaIndex,
CrewAI, AutoGen, or raw-SDK agents. No framework lock-in.

[langship.sh](https://langship.sh) · [github.com/open-gitagent/langship.sh](https://github.com/open-gitagent/langship.sh) · [CLI](./langship-cli/) · Apache 2.0

</div>

---

- [What you get](#what-you-get)
- [5 minutes to a green run](#5-minutes-to-a-green-run)
- [Architecture](#architecture)
- [Repo map](#repo-map)
- [The `langship` CLI](#the-langship-cli)
- [Concepts](#concepts)
- [Nodes](#nodes)
- [Reference — env vars & make targets](#reference)
- [Contributing & community](#contributing--community)
- [License](#license)

---

## What you get

| | |
|---|---|
| **Pipelines as graphs** | Drag-and-drop CI/CD nodes — Trigger → Build → Scan/SAST → Eval → Policy → Approval → Deploy → Promote → Rollback. n8n-shaped JSON on disk; YAML in git is the source of truth. |
| **Governance is a node** | Approvals, policy checks, eval gates, PII/secret scans are first-class, reorderable steps in the graph — not middleware you can't see. |
| **Any runtime, one pipeline** | Same definition deploys to K8s, Bedrock AgentCore, or Vertex Agent Engine. (Today the Deploy node ships to **Bedrock AgentCore** end-to-end; K8s / Vertex are stubbed.) |
| **Durable by construction** | Restate journals every node (`restate.Run("node:<name>", fn)`) — crash-safe replay, awakeable-based human approvals (timeout → auto-reject). |
| **GitOps promotion** | A Promote node opens/merges a PR `fromBranch → toBranch` on the agent's repo; the merge fires the next environment's pipeline. Promotion is an auditable event. |
| **Real OCI builds** | BuildKit solves your Dockerfile against the cloned repo, pushes to GHCR or any registry (private-repo PAT support). Mirror to N registries with the Push node. |
| **Operate, don't just deploy** | Live SSE log streams + canvas-overlay status rings; per-node logs archived to S3-compatible storage. |
| **Self-hosted, end-to-end** | Your cloud credentials, agent code, and run history never leave your network. Secrets AES-GCM sealed at rest. |
| **CLI-first** | `langship` — agents, envs, pipelines, creds, runs from your terminal. `git push` to ship. |

---

## 5 minutes to a green run

**0. Start everything.** Base compose bundles every service `flow` needs —
`mongo, restate, buildkitd, registry, minio, flow, web`.

```sh
docker compose up
```

| | URL |
|---|---|
| UI | http://localhost:3000 |
| API | http://localhost:8090 |
| Restate | `:8081` ingress · `:9070` admin |
| BuildKit | `tcp://127.0.0.1:1234` |
| Registry | `127.0.0.1:5050` (host port; buildkitd pushes to `registry:5000` internally) |
| MinIO | `127.0.0.1:9000` (S3 API) · `:9001` console (`minio` / `minio12345`) |

> If a sibling stack already owns one of those host ports, stop it or override the
> mapping in a `compose.override.yml`.

**1. Install the CLI and point it at the API.**

```sh
pip install -e ./langship-cli          # optional: pip install pyyaml  (for -o yaml)
langship login --api-url http://localhost:8090
```

**2. Register an agent (a git repo) and push a pipeline.**

```sh
langship agents create --repo https://github.com/you/your-agent --pat ghp_...
langship pipelines push examples/hello.json        # prints the new pipeline id
```

**3. Wire it into an environment, follow it, run it.**

```sh
langship envs create dev -d "Auto-deploy on push"
langship envs add-pipeline dev <pipelineId>
langship agents follow-env <agentId> dev
langship agents trigger <agentId>                  # → prints execution id(s)
```

**4. Watch it run.**

```sh
langship runs logs <executionId> -f                # live SSE stream
# or open the UI: http://localhost:3000/executions/view?id=<executionId>
```

That's the loop: `agent → env → pipeline → trigger → durable run → status`.

### Hot-reload dev (three terminals)

```sh
# 1) backing services only
docker compose up -d mongo restate            # + buildkitd/registry from the overlay

# 2) Go API with air — rebuilds on .go change
make watch                                    # or `make serve` for a stable binary

# 3) Next dev server with HMR; /api proxies to :8090
make dev
```

`make watch` pre-exports env defaults matching the compose host ports — override
any at the CLI, e.g. `make watch MINIO_ENDPOINT=...`. Set `FLOW_SECRET_KEY` in
your shell before touching anything credential/environment-related (the API
refuses credential writes without it).

---

## Architecture

Three layers, all run by you:

```
            ┌──────────────────────────────────────────────────────────┐
  CLI ──────►  API / control plane   (Go — pkg/api)                     │
  UI  ──────►   REST + SSE · agents/envs/pipelines/creds/runs · webhooks │
            └─────────────┬────────────────────────────────────────────┘
                          │ RunAsync
            ┌─────────────▼────────────────────────────────────────────┐
            │  Orchestration   (Restate cluster + worker)              │
            │  DAG walk (pkg/orchestrator) → executors (pkg/executors) │
            │  every node = restate.Run("node:<name>", fn)            │
            └─────────────┬────────────────────────────────────────────┘
                          │
            ┌─────────────▼────────────────────────────────────────────┐
            │  Data:  MongoDB  (pipelines · runs · agents · creds ·    │
            │                   environments)                          │
            │         MinIO / S3  (archived per-node logs, artifacts)  │
            │         Postgres  — Restate's backing store ONLY         │
            │         pkg/secrets  — AES-GCM seal/open (FLOW_SECRET_KEY)│
            └──────────────────────────────────────────────────────────┘
                          ▲
            GitHub webhook │  /webhooks/github/{id}  (HMAC-verified)
                           │  push → branch filter → dispatch run(s)
```

A run's lifecycle: webhook (or `langship agents trigger`) → the dispatcher walks
the agent's followed environments, applies each pipeline's branch filter, stamps
`agentId / environment / fromBranch` into the trigger payload, and calls
`orchestrator.RunAsync` → the DAG walker runs nodes in topological order, each
wrapped in `restate.Run` → terminal status written back to Mongo `runs` → SSE
clients (`/api/executions/{id}/stream`) get `node_started / node_log /
node_completed / node_error / done` events live.

> **Why these choices** — Restate gives crash-safe journaling + awakeables (human
> approval that survives a restart) for free; Mongo is the app store; Postgres is
> *only* Restate's persistence and is never touched by app code; BuildKit does
> real OCI builds without a Docker daemon. See [aude.md](./aude.md) for the full
> rationale.

---

## Repo map

```
cmd/flow/            the `flow` server binary (API + Restate worker entry point)
pkg/
  api/               REST + SSE handlers (agents, envs, pipelines, creds, runs, webhooks)
  orchestrator/      DAG walk; Approval is special-cased out of restate.Run (it
                     calls restate.Set/Clear directly)
  engine/            execution context, ExecutionEvent, the executor lookup
  executors/         node implementations + the registry:
                       trigger · build · push · sast · imagescan · approval ·
                       promote · deploy · (test/eval/policy/rollback stubs)
  awsdeploy/         AWS Bedrock AgentCore adapter — STS AssumeRole, idempotent
                     ECR + IAM bootstrap, control-plane SigV4, endpoint wait
  github/            REST helpers — webhook install/verify, PRs, merges
  storage/           Mongo-backed stores: pipelines, runs, agents, credentials,
                     environments
  secrets/           AES-GCM SealString/OpenString keyed off FLOW_SECRET_KEY
web/                 Next.js 15 UI (static export) — canvas, runs, agents,
                     environments, credentials
langship-cli/        the `langship` Python CLI (Typer / Rich / httpx)
examples/            sample pipeline JSON
```

---

## The `langship` CLI

The daily driver for agent devs; the bootstrap surface for platform engineers.

```sh
pip install -e ./langship-cli                # + pip install pyyaml  for -o yaml
langship login --api-url http://localhost:8090   # saved to ~/.langship/config.toml

# the loop
langship agents create --repo https://github.com/you/agent --pat ghp_...
langship pipelines push prod.yaml --id <pipelineId>     # create-or-update from a file
langship envs create prod -d "Strict gates"
langship envs add-pipeline prod <pipelineId>
langship envs reorder prod <pid1> <pid2> <pid3>          # promotion order
langship agents follow-env <agentId> prod
langship agents trigger <agentId>
langship runs logs <executionId> -f

# credentials (server needs FLOW_SECRET_KEY)
langship creds create prod-aws --type aws \
  --aws-region us-east-1 --aws-account 123456789012 \
  --aws-role-arn arn:aws:iam::123456789012:role/FlowDeployRole
```

Command groups: `agents`, `envs`, `pipelines`, `creds`, `runs` — each with
`--help`. `-o json` / `-o yaml` on list/get commands. `LANGSHIP_API_URL` /
`LANGSHIP_TOKEN` override the saved config. Full reference:
[`langship-cli/README.md`](./langship-cli/README.md).

---

## Concepts

- **Agent** — a registered git repo (URL + PAT). One-click GitHub webhook
  install; `/webhooks/github/{id}` verifies the HMAC signature and dispatches runs
  on push. An agent **follows environments** (`agent.environments[]`); triggering
  it runs the pipelines of every followed env. Agents may carry per-agent
  credential overrides.
- **Environment** — a named, **ordered list of pipelines** (the promotion
  sequence; reorderable). Global. Purely a sequencing container — per-deploy
  config lives on the nodes, not the env. `dev` / `staging` / `prod` / custom.
- **Pipeline** — a DAG of nodes built on the canvas (n8n-shape JSON underneath),
  stored in Mongo, loaded fresh per run. The Trigger node carries `fromBranch` /
  `toBranch`; a per-pipeline branch filter decides which pipelines run for a given
  push.
- **Credential** — a named record (`aws` / `gcp` / `kv`) in a global pool, with
  optional per-agent overrides. Secret fields are AES-GCM sealed at rest with
  `FLOW_SECRET_KEY`. Deploy / Push look one up by name.
- **Run** — one execution of a pipeline. Restate journals each node. Terminal
  status is written back to Mongo's `runs` collection. The dispatcher stamps
  `agentId`, `environment`, and `fromBranch` into the trigger payload; each node
  emits a `__<node>` summary object on its output items.
- **Live view** — `/executions/view?id=…` subscribes to
  `/api/executions/{id}/stream` (SSE) for `node_started`, `node_completed`,
  `node_error`, **`node_log`**, and `done` events; the canvas overlays status
  rings on each node.

---

## Nodes

| Node | What it does |
|---|---|
| **Trigger** | Entry point; carries `fromBranch` / `toBranch` for the branch filter + Promote. |
| **Build** | Clones the agent repo (`fromBranch`), builds an OCI image via BuildKit (`mode: docker`) or runs `/bin/sh -c <command>` in the clone (`mode: shell`). GHCR auth uses the agent's PAT (`write:packages`); `localhost:*` / `registry:*` are anonymous + insecure. Streams BuildKit's plain-mode progress as `node_log` events. |
| **Push** | Mirrors the built image to one or more registries (go-containerregistry's `crane`). |
| **SAST / ImageScan** | Sibling-container scanners — trivy / semgrep / gitleaks / SonarCloud / grype — over the source / image. Configurable severity threshold and fail-on-finding. |
| **Approval** | Pauses on a Restate awakeable until resumed via `POST /api/executions/{id}/resume` (UI or `langship`). `method: ui \| quorum \| auto`; optional `timeoutSeconds` → auto-reject. Two outputs: approved (0) / rejected (1). |
| **Promote** | Opens or merges a PR `fromBranch → toBranch` on the agent's repo via the GitHub API — idempotent (re-finds an existing PR). Modes: `open-pr` / `merge` / `merge-pr`. Emits `__promote` with the PR number / URL. The merge fires the next env's pipeline. |
| **Deploy** | Deploys the upstream Push image to **AWS Bedrock AgentCore** (`target: agentcore`; k8s / vertex are stubs). Looks up an `aws` credential by name, assumes the cross-account role, idempotently provisions the ECR repo + the shared `agentcore-runtime-role` IAM role, creates/updates the runtime, waits for the endpoint to be `READY`, and emits `__deploy` with the public invoke URL. |
| **Test / Eval / Policy / Rollback** | Stubbed for now — visible on the canvas, no-op executors. |

Adding a node? See the "Adding a node executor" section in
[CONTRIBUTING.md](./CONTRIBUTING.md).

---

## Reference

### Env vars (the `flow` process — `./bin/flow serve`, `make watch`, or compose)

| Var | Default | Notes |
|---|---|---|
| `FLOW_ADDR` | `:8090` | API listen address |
| `FLOW_CORS_ORIGINS` | `*` (compose: `http://localhost:3000`) | CSV allowlist |
| `FLOW_PUBLIC_URL` | (empty) | Externally-reachable base URL for webhook callback URLs. Set to your `cloudflared` tunnel for GitHub webhooks. |
| `FLOW_SECRET_KEY` | (unset → credential writes refused) | **Required for production.** Master key for AES-256-GCM encryption of sensitive data: agent PAT tokens, AWS keys, GCP service accounts, KV secrets. Any string; hashed to 32 bytes via SHA-256. **CRITICAL: Losing this key makes all sealed credentials unrecoverable.** Store securely (e.g., HashiCorp Vault, cloud KMS). For dev/test: any non-empty string. |
| `MONGO_URI` | (required; compose: `mongodb://localhost:27017`) | |
| `MONGO_DB` | `flow` | |
| `RESTATE_INGRESS_URL` | `http://localhost:8081` | |
| `RESTATE_ADMIN_URL` | `http://localhost:9070` | |
| `RESTATE_SERVICE_ADDR` | `:9080` | Service-endpoint listen addr |
| `RESTATE_DEPLOYMENT_URI` | `http://host.docker.internal:9080` | How Restate reaches us; compose overrides to `http://flow:9080` |
| `BUILDKIT_HOST` | `tcp://127.0.0.1:1234` | BuildKit gRPC; compose: `tcp://buildkitd:1234` |
| `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_BUCKET` / `MINIO_USE_SSL` | `127.0.0.1:9000` / `minio` / `minio12345` / `flow-logs` / `false` | Archived per-node log storage |

CLI env: `LANGSHIP_API_URL`, `LANGSHIP_TOKEN` (override `~/.langship/config.toml`).

### Make targets

| | |
|---|---|
| `make build` | build the web bundle then the Go binary (`bin/flow`) |
| `make build-go` | Go binary only (expects `web/dist` to exist) |
| `make serve` | `build-go` then `./bin/flow serve` — stable binary |
| `make watch` | Go API with `air` (rebuilds on `.go` change), env defaults pre-exported |
| `make dev` | Next dev server with HMR (`/api` proxies to `:8090`) |
| `make web` | build the Next static export |
| `make test` / `make vet` / `make tidy` | `go test ./...` / `go vet ./...` / `go mod tidy` |

---

## Contributing & community

- **Issues & discussion** — [github.com/open-gitagent/langship.sh/issues](https://github.com/open-gitagent/langship.sh/issues) for bugs and feature requests. Search first.
- **Contributing** — [CONTRIBUTING.md](./CONTRIBUTING.md): dev setup, what to run before a PR, conventions, how to add a node executor. Contributions accepted under Apache 2.0.
- **Code of conduct** — [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) (Contributor Covenant). Report concerns to <khush@lyzr.ai>.
- **Security** — **do not** file public issues for vulnerabilities. See [SECURITY.md](./SECURITY.md) — report privately to <khush@lyzr.ai>.

## License

[Apache 2.0](./LICENSE)
