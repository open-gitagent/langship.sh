package executors

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/lyzrai/flow/pkg/engine"
	"github.com/lyzrai/flow/pkg/models"
	"github.com/lyzrai/flow/pkg/storage"
)

// BuildExecutor clones the agent's repo and either runs a user shell
// command (mode=shell) or solves a Dockerfile via BuildKit and pushes the
// resulting image (mode=docker). Used by the Langship Build pipeline node.
//
// Common parameters:
//   - mode             "docker" (default) | "shell"
//   - timeoutSeconds   number  hard timeout (default: 600)
//
// docker-mode parameters:
//   - dockerfile      string   path to Dockerfile (default "Dockerfile")
//   - context         string   build context dir relative to clone (default ".")
//   - imageName       string   image name; default "<owner>/<repo>" from agent
//   - registry        string   registry host (default "localhost:5000")
//   - platform        string   target platform (default "linux/amd64")
//   - buildArgs       string   "k=v, k=v" CSV
//
// shell-mode parameters:
//   - command         string   shell command (default: docker build .)
//   - workdir         string   relative dir inside the clone (default ".")
//
// Trigger payload (from the executing pipeline):
//   - agentId   string   (required) — provided by Server.dispatchAgent / webhook
//   - commit    string   sha to check out; empty = use the agent's configured ref
//   - ref       string   git ref override
//
// On success the executor emits each input item with a __build object
// describing the run. On failure it returns an error so the runner halts.
type BuildExecutor struct {
	Agents storage.AgentStore
}

func (e *BuildExecutor) Execute(ctx context.Context, node models.NodeDef, inputs [][]models.Item, _ *engine.ExecutionContext) (map[int][]models.Item, error) {
	if e.Agents == nil {
		return nil, errors.New("build executor: AgentStore not configured")
	}

	trigger := firstItem(inputs)
	agentID, _ := trigger["agentId"].(string)
	if agentID == "" {
		return nil, errors.New("build: trigger payload missing agentId")
	}
	a, err := e.Agents.Get(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("build: load agent %q: %w", agentID, err)
	}

	pat, err := a.GetPAT()
	if err != nil {
		return nil, fmt.Errorf("build: decrypt PAT: %w", err)
	}

	mode := strParam(node.Parameters, "mode", "docker")
	timeoutSec := intParam(node.Parameters, "timeoutSeconds", 600)
	if timeoutSec < 10 {
		timeoutSec = 10
	}
	if timeoutSec > 3600 {
		timeoutSec = 3600
	}

	commitSHA, _ := trigger["commit"].(string)
	// `git clone --branch` wants a bare name like "main"; if a webhook
	// payload (or older trigger record) carried "refs/heads/main", trim it
	// so the clone doesn't fail with "Remote branch refs/heads/main not
	// found in upstream origin".
	// Prefer the Trigger node's explicit `fromBranch` over the older
	// webhook `ref` field, then fall back to the agent's configured ref.
	ref := stripRefsHeads(strFirst(
		strFromAny(trigger["fromBranch"]),
		strFromAny(trigger["ref"]),
		a.Ref,
		"main",
	))

	cloneDir, cleanup, err := cloneRepo(ctx, a.RepoURL, a.Name, pat, ref, commitSHA, time.Duration(timeoutSec)*time.Second)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	hardCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	var (
		exitCode int
		logTail  string
		runErr   error
		summary  map[string]any
	)

	switch mode {
	case "shell":
		exitCode, logTail, runErr = runShellMode(hardCtx, node, a, cloneDir, ref, commitSHA)
		summary = map[string]any{
			"mode":      "shell",
			"command":   strParam(node.Parameters, "command", ""),
			"exit_code": exitCode,
			"log_tail":  logTail,
		}
	case "docker", "":
		summary, logTail, runErr = runDockerMode(hardCtx, node, a.Name, pat, cloneDir, ref, commitSHA)
	default:
		return nil, fmt.Errorf("build: unknown mode %q (want docker|shell)", mode)
	}

	out := make([]models.Item, 0, max(1, totalLen(inputs)))
	base := map[string]any{
		"agent_id":    agentID,
		"agent_name":  a.Name,
		"repo_url":    a.RepoURL,
		"ref":         ref,
		"commit":      commitSHA,
		"finished_at": time.Now().UTC(),
	}
	for k, v := range summary {
		base[k] = v
	}

	for _, in := range inputs {
		for _, item := range in {
			ci := copyItem(item)
			ci["__build"] = base
			out = append(out, ci)
		}
	}
	if len(out) == 0 {
		out = append(out, models.Item{"__build": base})
	}

	if runErr != nil {
		return nil, fmt.Errorf("build (%s) failed: %w", mode, runErr)
	}
	return map[int][]models.Item{0: out}, nil
}

// --- shell mode ----------------------------------------------------------

func runShellMode(ctx context.Context, node models.NodeDef, a *storage.Agent, cloneDir, ref, commitSHA string) (int, string, error) {
	command := strParam(node.Parameters, "command", "docker build -t agent:latest .")
	workdirRel := strParam(node.Parameters, "workdir", ".")
	workdir := filepath.Join(cloneDir, filepath.Clean("/"+workdirRel))

	env := append(os.Environ(),
		"AGENT_NAME="+sanitizeEnvValue(a.Name),
		"REPO_URL="+a.RepoURL,
		"REF="+ref,
		"COMMIT_SHA="+commitSHA,
	)

	slog.InfoContext(ctx, "build_run_shell",
		slog.String("node", node.Name),
		slog.String("command", command),
		slog.String("workdir", workdir),
	)

	return runShellCapture(ctx, workdir, env, command)
}

// --- docker (BuildKit) mode ---------------------------------------------

func runDockerMode(ctx context.Context, node models.NodeDef, agentName, pat, cloneDir, ref, commitSHA string) (map[string]any, string, error) {
	bkAddr := os.Getenv("BUILDKIT_HOST")
	if bkAddr == "" {
		// docker-compose publishes buildkitd on 127.0.0.1:1234, so a host
		// `make watch` can reach it without extra config. In docker-compose,
		// the flow service overrides this with tcp://buildkitd:1234.
		bkAddr = "tcp://127.0.0.1:1234"
	}

	dockerfile := strParam(node.Parameters, "dockerfile", "Dockerfile")
	contextRel := strParam(node.Parameters, "context", ".")
	imageName := strParam(node.Parameters, "imageName", "")
	registry := strParam(node.Parameters, "registry", "registry:5000")
	platform := strParam(node.Parameters, "platform", "linux/amd64")
	buildArgs := parseKVCSV(strParam(node.Parameters, "buildArgs", ""))

	if imageName == "" {
		imageName = agentName // "owner/repo"
	}
	tag := commitSHA
	if tag == "" {
		tag = "latest"
	}
	imageRef := fmt.Sprintf("%s/%s:%s", strings.TrimRight(registry, "/"), imageName, tag)

	contextDir := filepath.Join(cloneDir, filepath.Clean("/"+contextRel))

	auth, insecure := authForRegistry(registry, pat)

	slog.InfoContext(ctx, "build_run_docker",
		slog.String("node", node.Name),
		slog.String("addr", bkAddr),
		slog.String("dockerfile", dockerfile),
		slog.String("context", contextDir),
		slog.String("image", imageRef),
		slog.String("platform", platform),
		slog.Bool("insecure", insecure),
	)

	logTail, err := runDockerBuild(ctx, dockerBuildOpts{
		BuildKitAddr: bkAddr,
		ContextDir:   contextDir,
		Dockerfile:   dockerfile,
		ImageRef:     imageRef,
		Platform:     platform,
		BuildArgs:    buildArgs,
		Insecure:     insecure,
		RegistryAuth: auth,
	})

	summary := map[string]any{
		"mode":       "docker",
		"image":      imageRef,
		"dockerfile": dockerfile,
		"context":    contextRel,
		"platform":   platform,
		"build_args": buildArgs,
		"log_tail":   logTail,
	}
	return summary, logTail, err
}

// authForRegistry decides what creds to send to BuildKit for `registry`.
// ghcr.io: use the PAT (must include write:packages).
// The username is always "x-access-token" for GitHub token auth to ghcr.io.
// (Prior code using agentOwner() is no longer needed; x-access-token is the
// canonical dummy username GitHub accepts for PAT-based auth.)
// localhost:* and registry:* (compose-internal): anonymous + insecure.
// Anything else: anonymous; user can wire a real auth path later.
func authForRegistry(registry string, pat string) (map[string]registryCreds, bool) {
	host := registryHostname(registry)
	insecure := isInsecureRegistry(host)

	if host == "ghcr.io" && pat != "" {
		return map[string]registryCreds{
			"ghcr.io": {Username: "x-access-token", Password: pat},
		}, false
	}
	return map[string]registryCreds{}, insecure
}

func registryHostname(registry string) string {
	r := strings.TrimSpace(registry)
	r = strings.TrimPrefix(r, "https://")
	r = strings.TrimPrefix(r, "http://")
	if i := strings.Index(r, "/"); i >= 0 {
		r = r[:i]
	}
	return r
}

func isInsecureRegistry(host string) bool {
	h := strings.ToLower(host)
	if strings.HasPrefix(h, "localhost") || strings.HasPrefix(h, "127.") {
		return true
	}
	// Compose-internal service hostnames typically resolve only inside the
	// docker network; users override the registry default to point at one.
	if h == "registry" || strings.HasPrefix(h, "registry:") {
		return true
	}
	return false
}

// parseKVCSV parses "k=v, k2=v2" into a map.
func parseKVCSV(s string) map[string]string {
	out := map[string]string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.Index(p, "=")
		if eq <= 0 {
			continue
		}
		k := strings.TrimSpace(p[:eq])
		v := strings.TrimSpace(p[eq+1:])
		if k != "" {
			out[k] = v
		}
	}
	return out
}

// --- clone helper --------------------------------------------------------

// cloneRepo shallow-clones the repo into a fresh tmp dir, optionally
// checking out a specific commit. Returns (cloneDir, cleanupFn, err).
func cloneRepo(ctx context.Context, repoURL, agentName, pat, ref, commit string, timeout time.Duration) (string, func(), error) {
	cloneDir, err := os.MkdirTemp("", "flow-build-*")
	if err != nil {
		return "", nil, fmt.Errorf("build: tmp dir: %w", err)
	}
	cleanup := func() { os.RemoveAll(cloneDir) }

	cloneURL, err := authedCloneURL(repoURL, pat)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("build: clone url: %w", err)
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	slog.InfoContext(ctx, "build_clone",
		slog.String("agent", agentName),
		slog.String("ref", ref),
		slog.String("dir", cloneDir),
	)

	if err := runCmd(cctx, "", "git", "clone", "--depth", "1",
		"--branch", ref, cloneURL, cloneDir); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("git clone: %w", err)
	}
	if commit != "" {
		_ = runCmd(cctx, cloneDir, "git", "fetch", "--depth", "1", "origin", commit)
		if err := runCmd(cctx, cloneDir, "git", "checkout", commit); err != nil {
			slog.WarnContext(ctx, "build_checkout_failed",
				slog.String("commit", commit),
				slog.Any("error", err),
			)
		}
	}
	return cloneDir, cleanup, nil
}

// --- helpers (unchanged from previous version) ---------------------------

func authedCloneURL(repoURL, pat string) (string, error) {
	if pat == "" || strings.HasPrefix(repoURL, "git@") {
		return repoURL, nil
	}
	u, err := url.Parse(repoURL)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword("x-access-token", pat)
	return u.String(), nil
}

func runCmd(ctx context.Context, cwd string, name string, args ...string) error {
	logger := engine.NodeLoggerFromContext(ctx)
	logger.Log("$ " + name + " " + strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, name, args...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return err
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 512*1024)
	for scanner.Scan() {
		logger.Log(scanner.Text())
	}
	return cmd.Wait()
}

func runShellCapture(ctx context.Context, cwd string, env []string, command string) (int, string, error) {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	if cwd != "" {
		cmd.Dir = cwd
	}
	cmd.Env = env

	logger := engine.NodeLoggerFromContext(ctx)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return -1, "", err
	}
	cmd.Stderr = cmd.Stdout // merge

	if err := cmd.Start(); err != nil {
		return -1, "", err
	}

	var buf bytes.Buffer
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		logger.Log(line)
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	// Best-effort wait. Ignore scanner error since the cmd Wait is the
	// authoritative signal.
	err = cmd.Wait()
	tail := lastLines(buf.String(), 80)

	if err == nil {
		return 0, tail, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), tail, fmt.Errorf("exit %d: %s",
			exitErr.ExitCode(), oneLineSummary(tail))
	}
	return -1, tail, err
}

func firstItem(inputs [][]models.Item) models.Item {
	for _, in := range inputs {
		for _, it := range in {
			if len(it) > 0 {
				return it
			}
		}
	}
	return models.Item{}
}

func totalLen(inputs [][]models.Item) int {
	n := 0
	for _, in := range inputs {
		n += len(in)
	}
	return n
}

func strParam(p map[string]any, key, def string) string {
	if p == nil {
		return def
	}
	if v, ok := p[key].(string); ok && v != "" {
		return v
	}
	return def
}

func intParam(p map[string]any, key string, def int) int {
	if p == nil {
		return def
	}
	switch v := p[key].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return def
}

func strFromAny(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func strFirst(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

func oneLineSummary(s string) string {
	for _, line := range strings.Split(s, "\n") {
		l := strings.TrimSpace(line)
		if l != "" {
			if len(l) > 200 {
				return l[:200] + "…"
			}
			return l
		}
	}
	return ""
}

// stripRefsHeads turns "refs/heads/main" into "main"; passes any other
// shape through unchanged. Tags ("refs/tags/v1") would still need a
// different clone strategy (--branch works for both branches and tags so
// we leave those alone).
func stripRefsHeads(s string) string {
	const p = "refs/heads/"
	if strings.HasPrefix(s, p) {
		return s[len(p):]
	}
	return s
}

func sanitizeEnvValue(s string) string {
	r := strings.NewReplacer("\n", " ", "\r", " ", "\x00", "")
	return r.Replace(s)
}
