// Package storage provides persistent stores for pipelines and execution
// history. The default backend is MongoDB; the interfaces stay narrow so an
// alternative backend (SQLite, Postgres) can be slotted in later.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/lyzrai/flow/pkg/secrets"
)

// ErrNotFound is returned when a queried document does not exist. API
// handlers should map this to HTTP 404.
var ErrNotFound = errors.New("not found")

// ErrAlreadyExists is returned by Create when a uniqueness constraint
// (e.g. credential name) would be violated. API handlers should map
// this to HTTP 409.
var ErrAlreadyExists = errors.New("already exists")

// Pipeline is the persisted shape of a pipeline (formerly "flow") that the
// API stores and returns to the UI. Definition is the n8n-format JSON.
type Pipeline struct {
	ID         string          `json:"id"          bson:"_id"`
	Name       string          `json:"name"        bson:"name"`
	Definition json.RawMessage `json:"definition"  bson:"definition"`
	NodeCount  int             `json:"nodeCount"   bson:"node_count"`
	Status     string          `json:"status"      bson:"status,omitempty"`
	CreatedAt  time.Time       `json:"createdAt"   bson:"created_at"`
	UpdatedAt  time.Time       `json:"updatedAt"   bson:"updated_at"`
}

// PipelineStore persists pipeline definitions.
type PipelineStore interface {
	Create(ctx context.Context, p *Pipeline) error
	Get(ctx context.Context, id string) (*Pipeline, error)
	Update(ctx context.Context, p *Pipeline) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]*Pipeline, error)
}

// Run is a single execution attempt against a pipeline. Captured at submit
// time; status/outputs are filled in later when the orchestrator finishes.
type Run struct {
	ID          string          `json:"id"                 bson:"_id"`           // execution_id
	PipelineID  string          `json:"pipelineId"         bson:"pipeline_id"`
	PipelineName string         `json:"pipelineName"       bson:"pipeline_name"`
	Status      string          `json:"status"             bson:"status"`
	StartedAt   time.Time       `json:"startedAt"          bson:"started_at"`
	FinishedAt  *time.Time      `json:"finishedAt,omitempty" bson:"finished_at,omitempty"`
	TriggerData json.RawMessage `json:"triggerData,omitempty" bson:"trigger_data,omitempty"`
	Outputs     json.RawMessage `json:"outputs,omitempty"     bson:"outputs,omitempty"`
	NodeOutputs json.RawMessage `json:"nodeOutputs,omitempty" bson:"node_outputs,omitempty"`
	Errors      []string        `json:"errors,omitempty"      bson:"errors,omitempty"`
}

// RunStore persists execution history.
type RunStore interface {
	Insert(ctx context.Context, r *Run) error
	UpdateStatus(ctx context.Context, id, status string) error
	Complete(ctx context.Context, id, status string, outputs, nodeOutputs json.RawMessage, errMsg string) error
	Get(ctx context.Context, id string) (*Run, error)
	ListByPipeline(ctx context.Context, pipelineID string, limit int) ([]*Run, error)
	List(ctx context.Context, limit int) ([]*Run, error)
}

// AuthStatus reflects the result of the most recent PAT/repo auth probe.
type AuthStatus string

const (
	AuthUntested AuthStatus = "untested"
	AuthOK       AuthStatus = "ok"
	AuthFailed   AuthStatus = "failed"
)

// CredentialType discriminates the shape of a credential record. Only
// fields belonging to the matching type should be populated; the rest
// stay zero-valued.
type CredentialType string

const (
	CredentialAWS CredentialType = "aws"
	CredentialGCP CredentialType = "gcp"
	CredentialKV  CredentialType = "kv"
)

// Credential is a named credential record attached to an agent. The
// Deploy node (and any other node that needs cloud creds) looks one up
// by Name. Secret fields are encrypted at rest with pkg/secrets; the API
// layer never returns them — only `HasSecret` flags.
//
// Fields are deliberately flat instead of `union { aws, gcp, kv }` so
// the Mongo schema stays simple and an upgrade to a new type only adds
// fields without rewriting the doc shape.
type Credential struct {
	ID   string         `json:"id"   bson:"id"`
	Name string         `json:"name" bson:"name"` // unique within an agent
	Type CredentialType `json:"type" bson:"type"`

	// AWS — non-secret. The Flow host's own identity AssumeRoles into
	// AwsCrossAccountRoleArn at deploy time.
	AwsRegion              string `json:"awsRegion,omitempty"              bson:"aws_region,omitempty"`
	AwsAccountID           string `json:"awsAccountId,omitempty"           bson:"aws_account_id,omitempty"`
	AwsCrossAccountRoleArn string `json:"awsCrossAccountRoleArn,omitempty" bson:"aws_cross_account_role_arn,omitempty"`

	// GCP — projectId / location are non-secret. Service account JSON is.
	GcpProjectID            string `json:"gcpProjectId,omitempty"   bson:"gcp_project_id,omitempty"`
	GcpLocation             string `json:"gcpLocation,omitempty"    bson:"gcp_location,omitempty"`
	GcpServiceAccountSealed string `json:"-"                        bson:"gcp_sa_sealed,omitempty"`

	// Generic key-value store. Each value is sealed independently so we
	// can return a list of keys publicly without leaking values.
	KvSealed map[string]string `json:"-" bson:"kv_sealed,omitempty"`

	CreatedAt time.Time `json:"createdAt" bson:"created_at"`
	UpdatedAt time.Time `json:"updatedAt" bson:"updated_at"`
}

// Agent is an agent repo registered with Langship. The PAT and webhook
// secret are stored server-side encrypted; the API layer scrubs them before the
// record leaves the boundary (see pkg/api/agents.go).
type Agent struct {
	ID                 string     `json:"id"                bson:"_id"`
	Name               string     `json:"name"              bson:"name"`
	RepoURL            string     `json:"repoUrl"           bson:"repo_url"`
	Ref                string     `json:"ref,omitempty"     bson:"ref,omitempty"`
	PATSealed          string     `json:"-"                 bson:"pat_sealed,omitempty"`
	PAT                string     `json:"-"                 bson:"pat,omitempty"`
	WebhookID          int64      `json:"webhookId,omitempty"        bson:"webhook_id,omitempty"`
	WebhookSecret      string     `json:"-"                          bson:"webhook_secret,omitempty"`
	WebhookInstalledAt *time.Time `json:"webhookInstalledAt,omitempty" bson:"webhook_installed_at,omitempty"`
	AuthStatus         AuthStatus `json:"authStatus,omitempty"        bson:"auth_status,omitempty"`
	AuthCheckedAt      *time.Time `json:"authCheckedAt,omitempty"     bson:"auth_checked_at,omitempty"`
	// Environments this agent follows by name. Triggering the agent runs
	// the pipelines of these environments (filtered by branch). Replaces
	// the older flat AttachedPipelines list — pipelines now live on the
	// environment, and agents subscribe to environments.
	Environments []string `json:"environments,omitempty" bson:"environments,omitempty"`

	// Named credentials — referenced by name from Deploy / future nodes.
	// These are agent-specific overrides of the global credential pool.
	Credentials []Credential `json:"credentials,omitempty" bson:"credentials,omitempty"`

	CreatedAt time.Time `json:"createdAt"         bson:"created_at"`
	UpdatedAt time.Time `json:"updatedAt"         bson:"updated_at"`
}

// GetPAT returns the decrypted PAT. Falls back to plaintext PAT field for
// backwards compatibility with agents created before encryption.
// Returns the decrypted value from PATSealed if available, otherwise plaintext PAT.
func (a *Agent) GetPAT() (string, error) {
	if a.PATSealed != "" {
		return secrets.OpenString(a.PATSealed)
	}
	return a.PAT, nil
}

// SetPAT encrypts and stores the PAT. Clears plaintext PAT field after encryption.
// This implements read-repair: plaintext PATs are encrypted on next write.
func (a *Agent) SetPAT(pat string) error {
	if pat == "" {
		a.PATSealed = ""
		a.PAT = ""
		return nil
	}
	sealed, err := secrets.SealString(pat)
	if err != nil {
		return err
	}
	a.PATSealed = sealed
	a.PAT = "" // Clear plaintext to enforce encryption
	return nil
}

// LookupCredential returns the agent's credential matching name (case-
// insensitive) and an ok flag. Convenience for executors.
func (a *Agent) LookupCredential(name string) (Credential, bool) {
	if a == nil {
		return Credential{}, false
	}
	for _, c := range a.Credentials {
		if strings.EqualFold(c.Name, name) {
			return c, true
		}
	}
	return Credential{}, false
}

// AgentStore persists agent registrations. Update mutates the entire
// record; callers do read-modify-write under their own consistency model.
type AgentStore interface {
	Create(ctx context.Context, a *Agent) error
	Get(ctx context.Context, id string) (*Agent, error)
	Update(ctx context.Context, a *Agent) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]*Agent, error)
}

// CredentialStore persists global (org-wide) credentials. Agents can
// override these by name with a record on agent.Credentials, but the
// global pool is the canonical place to define a credential once and
// reuse it across many agents/pipelines. Lookup is by name (the user-
// facing identifier — Deploy nodes reference creds by name, not ID).
type CredentialStore interface {
	Create(ctx context.Context, c *Credential) error
	GetByName(ctx context.Context, name string) (*Credential, error)
	Update(ctx context.Context, c *Credential) error
	Delete(ctx context.Context, name string) error
	List(ctx context.Context) ([]*Credential, error)
}

// Environment is a global, named deploy stage (dev / staging / prod /
// custom — the name is free-form). It is purely a sequencing container:
// it owns an ordered list of pipeline IDs (the promotion sequence) plus
// a description. Per-deploy config (credential, runtime target, approval
// method) lives on the nodes themselves, not here.
//
// Agents subscribe to environments by name (agent.Environments);
// triggering an agent runs the pipelines of the environments it follows
// (each pipeline still gated by its Trigger node's branch filter).
//
// A pipeline may appear in more than one environment.
type Environment struct {
	ID          string `json:"id"                    bson:"_id"`
	Name        string `json:"name"                  bson:"name"` // unique
	Description string `json:"description,omitempty"  bson:"description,omitempty"`

	// PipelineIDs is ordered — the order is the promotion sequence and is
	// reorderable via the API. Dispatch still applies each pipeline's own
	// branch filter; the order is the documented progression.
	PipelineIDs []string `json:"pipelineIds,omitempty" bson:"pipeline_ids,omitempty"`

	CreatedAt time.Time `json:"createdAt" bson:"created_at"`
	UpdatedAt time.Time `json:"updatedAt" bson:"updated_at"`
}

// HasPipeline reports whether the pipeline ID is brought into this env.
func (e *Environment) HasPipeline(pipelineID string) bool {
	if e == nil {
		return false
	}
	for _, p := range e.PipelineIDs {
		if p == pipelineID {
			return true
		}
	}
	return false
}

// EnvironmentStore persists global environments. Lookup is by name (the
// user-facing identifier — pipelines/agents reference envs by name).
type EnvironmentStore interface {
	Create(ctx context.Context, e *Environment) error
	GetByName(ctx context.Context, name string) (*Environment, error)
	Update(ctx context.Context, e *Environment) error
	Delete(ctx context.Context, name string) error
	List(ctx context.Context) ([]*Environment, error)
}
