package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lyzrai/flow/pkg/engine"
	"github.com/lyzrai/flow/pkg/github"
	"github.com/lyzrai/flow/pkg/models"
	"github.com/lyzrai/flow/pkg/orchestrator"
	"github.com/lyzrai/flow/pkg/storage"
)

// PublicCredential is the scrubbed shape of a stored credential. We
// return non-secret fields (region, accountId, role ARN, kv keys) and
// boolean flags for any sealed values, never the sealed bytes themselves.
type PublicCredential struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Type      storage.CredentialType `json:"type"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`

	// AWS — all non-secret.
	AwsRegion              string `json:"awsRegion,omitempty"`
	AwsAccountID           string `json:"awsAccountId,omitempty"`
	AwsCrossAccountRoleArn string `json:"awsCrossAccountRoleArn,omitempty"`

	// GCP — projectId / location are non-secret. HasServiceAccount tells
	// the UI whether the SA JSON has been uploaded.
	GcpProjectID      string `json:"gcpProjectId,omitempty"`
	GcpLocation       string `json:"gcpLocation,omitempty"`
	HasServiceAccount bool   `json:"hasServiceAccount,omitempty"`

	// Generic kv — keys are surfaced; values never are.
	KvKeys []string `json:"kvKeys,omitempty"`
}

// Agent is the wire shape of an agent record. PAT and webhook secret are
// scrubbed; HasPAT and webhook fields surface only the safe parts.
type Agent struct {
	ID                 string             `json:"id"`
	Name               string             `json:"name"`
	RepoURL            string             `json:"repoUrl"`
	Ref                string             `json:"ref,omitempty"`
	HasPAT             bool               `json:"hasPat"`
	WebhookID          int64              `json:"webhookId,omitempty"`
	WebhookURL         string             `json:"webhookUrl,omitempty"`
	WebhookInstalled   bool               `json:"webhookInstalled"`
	WebhookInstalledAt *time.Time         `json:"webhookInstalledAt,omitempty"`
	AuthStatus         storage.AuthStatus `json:"authStatus,omitempty"`
	AuthCheckedAt      *time.Time         `json:"authCheckedAt,omitempty"`
	Environments       []string           `json:"environments,omitempty"`
	Credentials        []PublicCredential `json:"credentials,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func publicCredential(c storage.Credential) PublicCredential {
	out := PublicCredential{
		ID:        c.ID,
		Name:      c.Name,
		Type:      c.Type,
		CreatedAt: c.CreatedAt,
		UpdatedAt: c.UpdatedAt,
	}
	switch c.Type {
	case storage.CredentialAWS:
		out.AwsRegion = c.AwsRegion
		out.AwsAccountID = c.AwsAccountID
		out.AwsCrossAccountRoleArn = c.AwsCrossAccountRoleArn
	case storage.CredentialGCP:
		out.GcpProjectID = c.GcpProjectID
		out.GcpLocation = c.GcpLocation
		out.HasServiceAccount = c.GcpServiceAccountSealed != ""
	case storage.CredentialKV:
		out.KvKeys = make([]string, 0, len(c.KvSealed))
		for k := range c.KvSealed {
			out.KvKeys = append(out.KvKeys, k)
		}
	}
	return out
}

func (s *Server) publicAgent(a *storage.Agent) Agent {
	creds := make([]PublicCredential, 0, len(a.Credentials))
	for _, c := range a.Credentials {
		creds = append(creds, publicCredential(c))
	}
	return Agent{
		ID:                 a.ID,
		Name:               a.Name,
		RepoURL:            a.RepoURL,
		Ref:                a.Ref,
		HasPAT:             a.PAT != "",
		WebhookID:          a.WebhookID,
		WebhookURL:         s.webhookURLFor(a.ID),
		WebhookInstalled:   a.WebhookID != 0,
		WebhookInstalledAt: a.WebhookInstalledAt,
		AuthStatus:         a.AuthStatus,
		AuthCheckedAt:      a.AuthCheckedAt,
		Environments:       a.Environments,
		Credentials:        creds,
		CreatedAt:          a.CreatedAt,
		UpdatedAt:          a.UpdatedAt,
	}
}

func (s *Server) webhookURLFor(agentID string) string {
	if s.publicURL == "" {
		return ""
	}
	return s.publicURL + "/webhooks/github/" + agentID
}

// --- CRUD -----------------------------------------------------------------

func (s *Server) handleListAgents(w http.ResponseWriter, r *http.Request) {
	agents, err := s.agents.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]Agent, 0, len(agents))
	for _, a := range agents {
		out = append(out, s.publicAgent(a))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCreateAgent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RepoURL string `json:"repoUrl"`
		PAT     string `json:"pat"`
		Ref     string `json:"ref,omitempty"`
		Name    string `json:"name,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	repo := strings.TrimSpace(body.RepoURL)
	if repo == "" {
		writeError(w, http.StatusBadRequest, errors.New("repoUrl is required"))
		return
	}
	if _, err := url.Parse(repo); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid repoUrl: %w", err))
		return
	}

	now := time.Now().UTC()
	id := newID()
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = deriveAgentName(repo)
	}
	ref := strings.TrimSpace(body.Ref)
	if ref == "" {
		ref = "main"
	}

	a := &storage.Agent{
		ID:         id,
		Name:       name,
		RepoURL:    repo,
		Ref:        ref,
		AuthStatus: storage.AuthUntested,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := a.SetPAT(strings.TrimSpace(body.PAT)); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := s.agents.Create(r.Context(), a); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, s.publicAgent(a))
}

func (s *Server) handleGetAgent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	writeJSON(w, http.StatusOK, s.publicAgent(a))
}

func (s *Server) handleDeleteAgent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Best-effort uninstall webhook before deleting, so we don't leak
	// dangling hooks pointing at a dead agent ID.
	if a, err := s.agents.Get(r.Context(), id); err == nil && a.WebhookID != 0 {
		if repo, perr := github.ParseRepo(a.RepoURL); perr == nil {
			pat, err := a.GetPAT()
			if err != nil {
				slog.WarnContext(r.Context(), "delete_agent_pat_decrypt_failed",
					slog.String("agent_id", id),
					slog.Any("error", err))
			}
			if pat != "" {
				ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
				_ = github.NewClient(pat).UninstallWebhook(ctx, repo, a.WebhookID)
				cancel()
			}
		}
	}
	if err := s.agents.Delete(r.Context(), id); err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- auth probe ----------------------------------------------------------

func (s *Server) handleTestAgentAuth(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	pat, err := a.GetPAT()
	if err != nil {
		slog.ErrorContext(r.Context(), "test_auth_pat_decrypt_failed",
			slog.String("agent_id", id),
			slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, errors.New("failed to retrieve agent credentials"))
		return
	}
	repo, err := github.ParseRepo(a.RepoURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	authErr := github.NewClient(pat).TestAuth(ctx, repo)

	now := time.Now().UTC()
	a.AuthCheckedAt = &now
	if authErr == nil {
		a.AuthStatus = storage.AuthOK
	} else {
		a.AuthStatus = storage.AuthFailed
	}
	a.UpdatedAt = now
	if uerr := s.agents.Update(r.Context(), a); uerr != nil {
		writeError(w, http.StatusInternalServerError, uerr)
		return
	}
	resp := map[string]any{
		"authStatus":    a.AuthStatus,
		"authCheckedAt": a.AuthCheckedAt,
	}
	if authErr != nil {
		resp["error"] = authErr.Error()
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- webhook install / uninstall -----------------------------------------

func (s *Server) handleInstallWebhook(w http.ResponseWriter, r *http.Request) {
	if s.publicURL == "" {
		writeError(w, http.StatusServiceUnavailable,
			errors.New("FLOW_PUBLIC_URL is not configured"))
		return
	}
	id := r.PathValue("id")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	pat, err := a.GetPAT()
	if err != nil {
		slog.ErrorContext(r.Context(), "pat_decrypt_failed",
			slog.String("agent_id", id),
			slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, errors.New("failed to retrieve agent credentials"))
		return
	}
	if pat == "" {
		writeError(w, http.StatusBadRequest, errors.New("agent has no PAT — re-create with one"))
		return
	}
	repo, err := github.ParseRepo(a.RepoURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Fresh secret per install so rotating is just "uninstall + install".
	secret, err := github.GenerateSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	callback := s.webhookURLFor(id)

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	hookID, err := github.NewClient(pat).InstallWebhook(ctx, repo, callback, secret)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	now := time.Now().UTC()
	a.WebhookID = hookID
	a.WebhookSecret = secret
	a.WebhookInstalledAt = &now
	a.UpdatedAt = now
	if err := s.agents.Update(r.Context(), a); err != nil {
		// Try to roll back the hook so we don't leak it.
		_ = github.NewClient(pat).UninstallWebhook(ctx, repo, hookID)
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, s.publicAgent(a))
}

func (s *Server) handleUninstallWebhook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	if a.WebhookID == 0 {
		writeError(w, http.StatusBadRequest, errors.New("no webhook installed"))
		return
	}
	pat, err := a.GetPAT()
	if err != nil {
		slog.ErrorContext(r.Context(), "pat_decrypt_failed",
			slog.String("agent_id", id),
			slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, errors.New("failed to retrieve agent credentials"))
		return
	}
	repo, err := github.ParseRepo(a.RepoURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if err := github.NewClient(pat).UninstallWebhook(ctx, repo, a.WebhookID); err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	a.WebhookID = 0
	a.WebhookSecret = ""
	a.WebhookInstalledAt = nil
	a.UpdatedAt = time.Now().UTC()
	if err := s.agents.Update(r.Context(), a); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, s.publicAgent(a))
}

// --- environment subscriptions -------------------------------------------

func (s *Server) handleAgentFollowEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	envName := r.PathValue("envName")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	if s.environments == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("environments store not configured"))
		return
	}
	if _, err := s.environments.GetByName(r.Context(), envName); err != nil {
		writeStorageErr(w, err, "environment not found")
		return
	}
	for _, e := range a.Environments {
		if strings.EqualFold(e, envName) {
			writeJSON(w, http.StatusOK, s.publicAgent(a)) // already following
			return
		}
	}
	a.Environments = append(a.Environments, envName)
	a.UpdatedAt = time.Now().UTC()
	if err := s.agents.Update(r.Context(), a); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, s.publicAgent(a))
}

func (s *Server) handleAgentUnfollowEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	envName := r.PathValue("envName")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	out := a.Environments[:0]
	for _, e := range a.Environments {
		if !strings.EqualFold(e, envName) {
			out = append(out, e)
		}
	}
	a.Environments = out
	a.UpdatedAt = time.Now().UTC()
	if err := s.agents.Update(r.Context(), a); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- manual trigger ------------------------------------------------------

// handleTriggerAgent dispatches runs across the agent's followed
// environments. Trigger data describes who/what triggered the run.
func (s *Server) handleTriggerAgent(w http.ResponseWriter, r *http.Request) {
	if s.orch == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("orchestrator not configured"))
		return
	}
	id := r.PathValue("id")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	if len(a.Environments) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("agent follows no environments"))
		return
	}
	triggerData := []map[string]any{{
		"source":    "manual",
		"agentId":   a.ID,
		"agentName": a.Name,
		"repoUrl":   a.RepoURL,
		"ref":       a.Ref,
	}}
	execIDs, failures, err := s.dispatchAgent(r.Context(), a, triggerData)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusAccepted
	if len(execIDs) == 0 && len(failures) > 0 {
		// Nothing dispatched — surface as 502 so the UI shows the error
		// card instead of silently navigating nowhere.
		status = http.StatusBadGateway
	}
	writeJSON(w, status, map[string]any{
		"executionIds": execIDs,
		"failures":     failures,
	})
}

// dispatchAgent fans out across the agent's followed environments. For
// each env it iterates the env's pipelines, applies the per-pipeline
// branch filter (for push triggers), stamps the env name + agent info
// into the trigger payload, and submits the run. Returns the execution
// IDs and a per-(env,pipeline) failure list so callers can surface skips.
func (s *Server) dispatchAgent(ctx context.Context, a *storage.Agent, trigger any) ([]string, []DispatchFailure, error) {
	// pushedBranch is set only for github_push triggers; it gates which
	// pipelines run (a pipeline's Trigger.fromBranch must match, or be a
	// wildcard). Manual triggers run every pipeline in every followed env.
	pushedBranch := ""
	isPush := false
	source := ""
	if items, ok := trigger.([]map[string]any); ok && len(items) > 0 {
		if src, _ := items[0]["source"].(string); src != "" {
			source = src
			if src == "github_push" {
				isPush = true
				if b, _ := items[0]["branch"].(string); b != "" {
					pushedBranch = b
				} else if b, _ := items[0]["ref"].(string); b != "" {
					pushedBranch = b
				}
			}
		}
	}

	var out []string
	var failures []DispatchFailure

	for _, envName := range a.Environments {
		env, err := s.environments.GetByName(ctx, envName)
		if err != nil {
			slog.WarnContext(ctx, "agent_dispatch_env_missing",
				slog.String("agent_id", a.ID), slog.String("env", envName), slog.Any("error", err))
			failures = append(failures, DispatchFailure{
				Environment: envName, Reason: "environment not found", Error: err.Error(),
			})
			continue
		}
		for _, pid := range env.PipelineIDs {
			p, err := s.pipelines.Get(ctx, pid)
			if err != nil {
				failures = append(failures, DispatchFailure{
					Environment: envName, PipelineID: pid, Reason: "pipeline not found", Error: err.Error(),
				})
				continue
			}
			wf, err := engine.ParseWorkflow(p.Definition)
			if err != nil {
				failures = append(failures, DispatchFailure{
					Environment: envName, PipelineID: pid, Reason: "parse failed", Error: err.Error(),
				})
				continue
			}
			if isPush {
				tb := pipelineTriggerBranch(wf)
				if tb != "" && tb != "*" && tb != pushedBranch {
					failures = append(failures, DispatchFailure{
						Environment: envName, PipelineID: pid, Reason: "branch filtered",
						Error: fmt.Sprintf("trigger fromBranch=%q ≠ pushed=%q", tb, pushedBranch),
					})
					continue
				}
			}

			// Per-(env, pipeline) trigger payload: clone the base items and
			// stamp the env name so downstream nodes (Deploy / Approval) can
			// inherit env defaults.
			perRunItems := stampTriggerEnv(trigger, envName)
			triggerJSON, _ := json.Marshal(perRunItems)
			triggerItems := make([]models.Item, 0, len(perRunItems))
			for _, m := range perRunItems {
				triggerItems = append(triggerItems, models.Item(m))
			}

			runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			execID, err := s.orch.RunAsync(runCtx, &orchestrator.RunRequest{
				RequestMeta: orchestrator.RequestMeta{WorkflowID: pid},
				Workflow:    wf,
				TriggerData: triggerItems,
			})
			cancel()
			if err != nil {
				failures = append(failures, DispatchFailure{
					Environment: envName, PipelineID: pid, Reason: "orchestrator submit", Error: err.Error(),
				})
				continue
			}
			startLogArchiver(context.Background(), s.logs, s.events, execID)
			s.runsBus.Publish(RunCreatedEvent{
				Type:         "run_created",
				ExecutionID:  execID,
				PipelineID:   pid,
				PipelineName: p.Name,
				AgentID:      a.ID,
				Environment:  envName,
				Source:       source,
				StartedAt:    time.Now().UTC(),
			})
			if s.runs != nil {
				_ = s.runs.Insert(ctx, &storage.Run{
					ID:           execID,
					PipelineID:   pid,
					PipelineName: p.Name,
					Status:       "running",
					StartedAt:    time.Now().UTC(),
					TriggerData:  triggerJSON,
				})
			}
			out = append(out, execID)
		}
	}
	return out, failures, nil
}

// stampTriggerEnv returns a copy of the trigger items (each a map) with
// `environment` set to envName. The base items are not mutated so the
// same trigger payload can be reused across environments.
func stampTriggerEnv(trigger any, envName string) []map[string]any {
	items, ok := trigger.([]map[string]any)
	if !ok || len(items) == 0 {
		return []map[string]any{{"environment": envName}}
	}
	out := make([]map[string]any, 0, len(items))
	for _, m := range items {
		cp := make(map[string]any, len(m)+1)
		for k, v := range m {
			cp[k] = v
		}
		cp["environment"] = envName
		out = append(out, cp)
	}
	return out
}

// DispatchFailure describes why a single (environment, pipeline) pair was
// skipped at dispatch time. Surfaced to API callers so trigger failures
// don't appear as silent no-ops.
type DispatchFailure struct {
	Environment string `json:"environment,omitempty"`
	PipelineID  string `json:"pipelineId,omitempty"`
	Reason      string `json:"reason"`
	Error       string `json:"error,omitempty"`
}

// --- webhook receiver ----------------------------------------------------

func (s *Server) handleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a, err := s.agents.Get(r.Context(), id)
	if err != nil {
		writeStorageErr(w, err, "agent not found")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := github.VerifySignature(r.Header.Get("X-Hub-Signature-256"), a.WebhookSecret, body); err != nil {
		slog.WarnContext(r.Context(), "github_webhook_signature_invalid",
			slog.String("agent_id", id),
			slog.Any("error", err),
		)
		writeError(w, http.StatusUnauthorized, errors.New("signature invalid"))
		return
	}

	event := r.Header.Get("X-GitHub-Event")
	switch event {
	case "ping":
		writeJSON(w, http.StatusOK, map[string]string{"message": "pong"})
		return
	case "push":
		// fall through
	default:
		// Acknowledge unknown events; nothing to dispatch.
		writeJSON(w, http.StatusOK, map[string]string{"message": "ignored"})
		return
	}

	push, err := github.ParsePushEvent(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Note: branch filtering happens per-pipeline in dispatchAgent — each
	// Trigger node's `fromBranch` decides whether that pipeline runs for
	// this push. The agent-level `Ref` is now only the default clone branch
	// (used by Build when nothing upstream specifies one), not a webhook
	// gate, so chains across branches (Promote main→dev fires the dev
	// pipeline) work correctly.

	if s.orch == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("orchestrator not configured"))
		return
	}

	// Normalize the ref to the bare branch name (`main`, not `refs/heads/main`)
	// so downstream nodes — particularly Build's git clone --branch — don't
	// have to know about Git's internal ref namespace. `fullRef` is kept for
	// nodes that want the original.
	branch := github.BranchFromRef(push.Ref)
	trigger := []map[string]any{{
		"source":    "github_push",
		"agentId":   a.ID,
		"agentName": a.Name,
		"repoUrl":   a.RepoURL,
		"ref":       branch,
		"branch":    branch,
		"fullRef":   push.Ref,
		"commit":    push.After,
		"pusher":    push.Pusher.Name,
	}}
	execIDs, failures, err := s.dispatchAgent(r.Context(), a, trigger)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"executionIds": execIDs,
		"failures":     failures,
	})
}

// --- helpers -------------------------------------------------------------

// deriveAgentName extracts an "org/repo" name from a git URL. Falls back to
// the URL itself if parsing fails.
func deriveAgentName(raw string) string {
	if r, err := github.ParseRepo(raw); err == nil {
		return r.String()
	}
	return strings.TrimSpace(raw)
}

// pipelineTriggerBranch returns the `fromBranch` parameter on the workflow's
// Trigger node, or "" if the workflow has no Trigger node or no fromBranch
// configured. Used to filter webhook dispatch so only pipelines matching the
// pushed branch run. If multiple Trigger nodes exist (rare — schema allows it
// but the orchestrator only feeds one), the first wins.
func pipelineTriggerBranch(wf *models.WorkflowDefinition) string {
	if wf == nil {
		return ""
	}
	for _, n := range wf.Nodes {
		if n.Type != "flow-nodes-base.trigger" {
			continue
		}
		if v, ok := n.Parameters["fromBranch"].(string); ok {
			return strings.TrimSpace(v)
		}
		return ""
	}
	return ""
}
