# langship

CLI for the Langship control plane. Drive agents, environments, pipelines,
credentials, and runs from your terminal — same API the web UI uses.

## Install

```bash
pip install -e /Users/khushpatel2002/langship-restate/langship-cli
# (once published: pip install langship)
```

Optional: `pip install pyyaml` to use `-o yaml` and YAML pipeline files.

## Setup

```bash
langship login --api-url http://localhost:8090
# or just: langship login   (prompts; default http://localhost:8090)
```

Saved to `~/.langship/config.toml`. Override per-invocation:

```bash
LANGSHIP_API_URL=http://localhost:8090 langship agents list
LANGSHIP_TOKEN=... langship agents list   # if/when the API requires auth
```

There is no built-in default URL — you must `login` (or set `LANGSHIP_API_URL`)
before any command other than `login`.

## Quick tour

```bash
# Agents
langship agents create --repo https://github.com/me/agent --pat ghp_...
langship agents list
langship agents get <agentId>
langship agents trigger <agentId>          # dispatch across followed envs

# Environments (named, ORDERED list of pipelines = the promotion sequence)
langship envs create dev -d "Auto-deploy on push"
langship pipelines push pipeline.json      # -> prints the new pipeline id
langship envs add-pipeline dev <pipelineId>
langship envs reorder dev <pid1> <pid2> <pid3>
langship envs get dev

# Agent follows an environment -> triggering it runs that env's pipelines
langship agents follow-env <agentId> dev
langship agents trigger <agentId>

# Credentials (global pool — server needs FLOW_SECRET_KEY set)
langship creds create prod-aws --type aws \
  --aws-region us-east-1 --aws-account 123456789012 \
  --aws-role-arn arn:aws:iam::123456789012:role/FlowDeployRole
langship creds list

# Runs
langship runs list
langship runs logs <executionId> -f         # stream live
langship runs get <executionId>
```

## Commands

```
langship login [--api-url URL] [--token TOK]
langship config-show
langship version

langship agents list [-o json|yaml]
langship agents get <agentId> [-o ...]
langship agents create --repo <url> [--pat ...] [--name ...] [--ref ...]
langship agents delete <agentId> [-y]
langship agents trigger <agentId>
langship agents follow-env <agentId> <env>
langship agents unfollow-env <agentId> <env>
langship agents test-auth <agentId>
langship agents webhook install <agentId>
langship agents webhook uninstall <agentId>

langship envs list [-o ...]
langship envs get <name> [-o ...]
langship envs create <name> [-d <description>]
langship envs update <name> -d <description>
langship envs delete <name> [-y]
langship envs add-pipeline <name> <pipelineId>
langship envs remove-pipeline <name> <pipelineId>
langship envs reorder <name> <pid> <pid> ...

langship pipelines list [-o ...]
langship pipelines get <pipelineId> [-o json|yaml|table]
langship pipelines push <file.json|.yaml> [--id <pipelineId>] [--name ...]
langship pipelines validate <file.json|.yaml>
langship pipelines delete <pipelineId> [-y]

langship creds list [-o ...]
langship creds get <name> [-o ...]
langship creds create <name> --type aws|gcp|kv [provider flags...]
langship creds delete <name> [-y]

langship runs list [--limit N] [--pipeline <id>] [-o ...]
langship runs get <executionId> [-o ...]
langship runs logs <executionId> [-f]
```

## Env vars

- `LANGSHIP_API_URL` — overrides the saved API URL
- `LANGSHIP_TOKEN` — overrides the saved auth token
