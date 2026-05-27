package executors

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lyzrai/flow/pkg/engine"
	"github.com/lyzrai/flow/pkg/github"
	"github.com/lyzrai/flow/pkg/models"
	"github.com/lyzrai/flow/pkg/storage"
)

// PromoteExecutor promotes the agent's source code by either opening a
// pull request from `fromBranch` → `toBranch`, or merging the two
// branches directly via the GitHub API. This is GitOps promotion, not
// image-tag promotion — promotion is recorded as a real commit / PR in
// the agent's repo so the audit trail lives where reviewers expect it.
//
// Source of branches (precedence):
//   1. node.parameters.fromBranch / toBranch
//   2. trigger payload `fromBranch` / `toBranch` (Trigger node carries these)
//   3. trigger payload `ref` for fromBranch
//   4. agent.Ref for fromBranch
//   5. "main" for fromBranch, error if no toBranch
//
// Modes:
//   - open-pr (default): POST /repos/.../pulls; idempotent (re-finds an
//     existing open PR for the same head/base pair).
//   - merge        : POST /repos/.../merges → fast-forward / merge-commit.
//   - merge-pr     : open the PR if needed, then PUT /pulls/{n}/merge.
//
// Parameters:
//   - mode             "open-pr" | "merge" | "merge-pr"   (default open-pr)
//   - fromBranch       string   override
//   - toBranch         string   override
//   - title            string   PR title (open-pr / merge-pr)
//   - body             string   PR body  (open-pr / merge-pr)
//   - mergeMethod      "merge" | "squash" | "rebase"      (merge-pr only)
//   - timeoutSeconds   number                              (default 60)
type PromoteExecutor struct {
	Agents storage.AgentStore
}

func (e *PromoteExecutor) Execute(ctx context.Context, node models.NodeDef, inputs [][]models.Item, _ *engine.ExecutionContext) (map[int][]models.Item, error) {
	if e.Agents == nil {
		return nil, errors.New("promote: AgentStore not configured")
	}
	logger := engine.NodeLoggerFromContext(ctx)

	trigger := firstItem(inputs)
	agentID, _ := trigger["agentId"].(string)
	if agentID == "" {
		return nil, errors.New("promote: trigger payload missing agentId")
	}
	a, err := e.Agents.Get(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("promote: load agent %q: %w", agentID, err)
	}
	pat, err := a.GetPAT()
	if err != nil {
		return nil, fmt.Errorf("promote: decrypt PAT: %w", err)
	}
	repo, err := github.ParseRepo(a.RepoURL)
	if err != nil {
		return nil, fmt.Errorf("promote: parse repo %q: %w", a.RepoURL, err)
	}
	if pat == "" {
		return nil, errors.New("promote: agent has no PAT (need 'repo' scope to open PRs / merge)")
	}

	mode := strings.ToLower(strParam(node.Parameters, "mode", "open-pr"))
	from := stripRefsHeads(resolveBranch(
		strParam(node.Parameters, "fromBranch", ""),
		strFromAny(trigger["fromBranch"]),
		strFromAny(trigger["ref"]),
		a.Ref,
		"main",
	))
	to := stripRefsHeads(resolveBranch(
		strParam(node.Parameters, "toBranch", ""),
		strFromAny(trigger["toBranch"]),
	))
	if to == "" {
		return nil, errors.New("promote: no toBranch (set on Trigger node or Promote node)")
	}
	if from == to {
		return nil, fmt.Errorf("promote: fromBranch and toBranch are both %q", from)
	}

	timeoutSec := intParam(node.Parameters, "timeoutSeconds", 60)
	hardCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	logger.Log(fmt.Sprintf("[promote:%s] %s/%s: %s → %s",
		mode, repo.Owner, repo.Name, from, to))

	cli := github.NewClient(pat)
	title := strParam(node.Parameters, "title", "")
	body := strParam(node.Parameters, "body", "")
	commitMsg := firstNonEmptyStr(title, fmt.Sprintf("Promote %s → %s", from, to))

	summary := map[string]any{
		"mode":        mode,
		"agentId":     agentID,
		"agentName":   a.Name,
		"repo":        repo.String(),
		"fromBranch":  from,
		"toBranch":    to,
		"finished_at": time.Now().UTC(),
	}

	switch mode {
	case "open-pr", "":
		pr, err := cli.OpenPullRequest(hardCtx, repo, from, to, title, body)
		if err != nil {
			return nil, fmt.Errorf("promote: %w", err)
		}
		logger.Log(fmt.Sprintf("[promote] PR #%d opened: %s", pr.Number, pr.HTMLURL))
		summary["prNumber"] = pr.Number
		summary["prUrl"] = pr.HTMLURL
		summary["prState"] = pr.State

	case "merge":
		sha, err := cli.MergeBranches(hardCtx, repo, to, from, commitMsg)
		if err != nil {
			return nil, fmt.Errorf("promote: %w", err)
		}
		if sha == "" {
			logger.Log("[promote] branches already up-to-date; nothing to merge")
			summary["upToDate"] = true
		} else {
			logger.Log(fmt.Sprintf("[promote] merged %s → %s as %s", from, to, sha))
			summary["mergeSha"] = sha
		}

	case "merge-pr":
		pr, err := cli.OpenPullRequest(hardCtx, repo, from, to, title, body)
		if err != nil {
			return nil, fmt.Errorf("promote (open): %w", err)
		}
		summary["prNumber"] = pr.Number
		summary["prUrl"] = pr.HTMLURL

		method := strings.ToLower(strParam(node.Parameters, "mergeMethod", "merge"))
		sha, err := cli.MergePullRequest(hardCtx, repo, pr.Number, commitMsg, method)
		if err != nil {
			return nil, fmt.Errorf("promote (merge PR #%d): %w", pr.Number, err)
		}
		logger.Log(fmt.Sprintf("[promote] PR #%d merged as %s", pr.Number, sha))
		summary["mergeSha"] = sha
		summary["mergeMethod"] = method

	default:
		return nil, fmt.Errorf("promote: unknown mode %q (want open-pr|merge|merge-pr)", mode)
	}

	out := make([]models.Item, 0)
	for _, in := range inputs {
		for _, it := range in {
			ci := copyItem(it)
			ci["__promote"] = summary
			out = append(out, ci)
		}
	}
	if len(out) == 0 {
		out = append(out, models.Item{"__promote": summary})
	}
	return map[int][]models.Item{0: out}, nil
}

// resolveBranch returns the first non-empty trimmed string in the given
// list. Empty by default — the caller decides whether that's an error.
func resolveBranch(candidates ...string) string {
	for _, c := range candidates {
		if s := strings.TrimSpace(c); s != "" {
			return s
		}
	}
	return ""
}

// firstNonEmptyStr is the strParam-friendly variant of strFirst that takes
// any number of strings (kept local so we don't conflict with Push's helper).
func firstNonEmptyStr(ss ...string) string {
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
