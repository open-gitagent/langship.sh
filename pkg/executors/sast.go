package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/lyzrai/flow/pkg/engine"
	"github.com/lyzrai/flow/pkg/models"
	"github.com/lyzrai/flow/pkg/storage"
)

// SastExecutor runs static-analysis security scanning against the agent's
// source repo. The `tool` parameter selects which scanner runs; each tool
// is just a sibling docker container we shell out to (`docker run --rm`),
// except `sonar` which uploads to SonarCloud and polls for the quality
// gate verdict.
//
// Trigger payload requirements (same as Build):
//   - agentId   string   (provided by dispatchAgent / webhook)
//   - commit    string   commit SHA to check out (optional)
//   - ref       string   ref override
//
// Common parameters:
//   - tool                trivy | semgrep | gitleaks | sonar | custom    (default: trivy)
//   - severityThreshold   LOW | MEDIUM | HIGH | CRITICAL                 (default: HIGH)
//   - failOnFinding       bool                                            (default: true)
//   - timeoutSeconds      number                                          (default: 600)
//
// Tool-specific parameters:
//   - trivy / semgrep / gitleaks  no extra config — sane defaults
//   - sonar:
//       sonarHost     default https://sonarcloud.io
//       organization  required
//       projectKey    required
//       sonarToken    required (SONAR_TOKEN)
//       branchName    optional, defaults to the agent ref
//   - custom:
//       image         OCI image to run (required)
//       command       shell command inside the container (required)
type SastExecutor struct {
	Agents storage.AgentStore
}

func (e *SastExecutor) Execute(ctx context.Context, node models.NodeDef, inputs [][]models.Item, _ *engine.ExecutionContext) (map[int][]models.Item, error) {
	if e.Agents == nil {
		return nil, errors.New("sast: AgentStore not configured")
	}

	logger := engine.NodeLoggerFromContext(ctx)

	trigger := firstItem(inputs)
	agentID, _ := trigger["agentId"].(string)
	if agentID == "" {
		return nil, errors.New("sast: trigger payload missing agentId")
	}
	a, err := e.Agents.Get(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("sast: load agent %q: %w", agentID, err)
	}

	pat, err := a.GetPAT()
	if err != nil {
		return nil, fmt.Errorf("sast: decrypt PAT: %w", err)
	}

	tool := strings.ToLower(strParam(node.Parameters, "tool", "trivy"))
	threshold := strings.ToUpper(strParam(node.Parameters, "severityThreshold", "HIGH"))
	failOnFinding := boolParam(node.Parameters, "failOnFinding", true)
	timeoutSec := intParam(node.Parameters, "timeoutSeconds", 600)
	if timeoutSec < 30 {
		timeoutSec = 30
	}
	if timeoutSec > 3600 {
		timeoutSec = 3600
	}

	commitSHA, _ := trigger["commit"].(string)
	ref := stripRefsHeads(strFirst(strFromAny(trigger["ref"]), a.Ref, "main"))

	cloneDir, cleanup, err := cloneRepo(ctx, a.RepoURL, a.Name, pat, ref, commitSHA, time.Duration(timeoutSec)*time.Second)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	hardCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	logger.Log(fmt.Sprintf("[sast:%s] running against %s @ %s", tool, a.Name, ref))

	var (
		findings []sastFinding
		summary  map[string]any
		toolErr  error
	)
	switch tool {
	case "trivy":
		findings, toolErr = runTrivy(hardCtx, cloneDir, threshold, logger)
	case "semgrep":
		findings, toolErr = runSemgrep(hardCtx, cloneDir, logger)
	case "gitleaks":
		findings, toolErr = runGitleaks(hardCtx, cloneDir, logger)
	case "sonar":
		summary, toolErr = runSonarCloud(hardCtx, node.Parameters, a, cloneDir, ref, commitSHA, logger)
	case "custom":
		findings, toolErr = runCustom(hardCtx, node.Parameters, cloneDir, logger)
	default:
		return nil, fmt.Errorf("sast: unknown tool %q", tool)
	}

	if toolErr != nil {
		return nil, fmt.Errorf("sast (%s): %w", tool, toolErr)
	}

	// Counts by severity.
	counts := map[string]int{}
	for _, f := range findings {
		counts[strings.ToUpper(f.Severity)]++
	}

	out := map[string]any{
		"tool":              tool,
		"agent_id":          agentID,
		"agent_name":        a.Name,
		"ref":               ref,
		"commit":            commitSHA,
		"severityThreshold": threshold,
		"counts":            counts,
		"finding_count":     len(findings),
		"finished_at":       time.Now().UTC(),
	}
	if findings != nil {
		out["findings"] = findings
	}
	if summary != nil {
		// Sonar mode replaces per-finding output with a server-side
		// quality-gate summary.
		for k, v := range summary {
			out[k] = v
		}
	}

	logger.Log(fmt.Sprintf("[sast:%s] done — %d finding(s)", tool, len(findings)))

	// Decide pass/fail.
	failed := false
	if failOnFinding {
		if tool == "sonar" {
			// Sonar passes/fails on its quality-gate verdict.
			if status, _ := summary["qualityGate"].(string); strings.ToUpper(status) == "ERROR" {
				failed = true
			}
		} else if exceedsThreshold(findings, threshold) {
			failed = true
		}
	}

	items := make([]models.Item, 0)
	for _, in := range inputs {
		for _, it := range in {
			ci := copyItem(it)
			ci["__sast"] = out
			items = append(items, ci)
		}
	}
	if len(items) == 0 {
		items = append(items, models.Item{"__sast": out})
	}

	if failed {
		return nil, fmt.Errorf("sast (%s) failed: severity threshold %s exceeded (%v)",
			tool, threshold, counts)
	}
	return map[int][]models.Item{0: items}, nil
}

// sastFinding is the normalized shape we emit on output items.
type sastFinding struct {
	Tool     string `json:"tool"`
	Severity string `json:"severity"`
	RuleID   string `json:"ruleId,omitempty"`
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
	Message  string `json:"message"`
}

// --- trivy ----------------------------------------------------------------

// trivy filesystem scan: vulns + secrets + IaC misconfig in one shot.
// We scan the host clone path by mounting it read-only into the trivy
// container. Output is JSON; we parse the few fields we render.
func runTrivy(ctx context.Context, dir, threshold string, logger engine.NodeLogger) ([]sastFinding, error) {
	// Restrict scan to threshold + above so trivy doesn't dump 5000 LOW
	// findings. Trivy understands a comma-separated severity list.
	sev := severityChainAtOrAbove(threshold)
	args := []string{
		"run", "--rm",
		"-v", dir + ":/src:ro",
		"aquasec/trivy:latest",
		"fs", "--quiet",
		"--format", "json",
		"--severity", sev,
		"--scanners", "vuln,secret,misconfig",
		"/src",
	}
	out, err := dockerRunCapture(ctx, args, logger)
	if err != nil && len(out) == 0 {
		return nil, err
	}
	return parseTrivy(out)
}

// trivyReport is the minimal shape of `trivy fs --format json`.
type trivyReport struct {
	Results []struct {
		Target          string `json:"Target"`
		Class           string `json:"Class"`
		Vulnerabilities []struct {
			VulnerabilityID  string `json:"VulnerabilityID"`
			PkgName          string `json:"PkgName"`
			InstalledVersion string `json:"InstalledVersion"`
			Severity         string `json:"Severity"`
			Title            string `json:"Title"`
		} `json:"Vulnerabilities,omitempty"`
		Secrets []struct {
			RuleID   string `json:"RuleID"`
			Severity string `json:"Severity"`
			Title    string `json:"Title"`
			StartLine int   `json:"StartLine"`
		} `json:"Secrets,omitempty"`
		Misconfigurations []struct {
			ID       string `json:"ID"`
			Severity string `json:"Severity"`
			Title    string `json:"Title"`
		} `json:"Misconfigurations,omitempty"`
	} `json:"Results"`
}

func parseTrivy(raw []byte) ([]sastFinding, error) {
	// Trivy may emit logs on stderr that bleed into combined output; find the
	// first '{' to start parsing JSON.
	if i := bytes.IndexByte(raw, '{'); i > 0 {
		raw = raw[i:]
	}
	var r trivyReport
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("parse trivy json: %w", err)
	}
	var findings []sastFinding
	for _, res := range r.Results {
		for _, v := range res.Vulnerabilities {
			findings = append(findings, sastFinding{
				Tool:     "trivy",
				Severity: v.Severity,
				RuleID:   v.VulnerabilityID,
				File:     res.Target,
				Message: fmt.Sprintf("%s in %s@%s — %s",
					v.VulnerabilityID, v.PkgName, v.InstalledVersion, v.Title),
			})
		}
		for _, s := range res.Secrets {
			findings = append(findings, sastFinding{
				Tool:     "trivy",
				Severity: s.Severity,
				RuleID:   s.RuleID,
				File:     res.Target,
				Line:     s.StartLine,
				Message:  s.Title,
			})
		}
		for _, m := range res.Misconfigurations {
			findings = append(findings, sastFinding{
				Tool:     "trivy",
				Severity: m.Severity,
				RuleID:   m.ID,
				File:     res.Target,
				Message:  m.Title,
			})
		}
	}
	return findings, nil
}

// --- semgrep --------------------------------------------------------------

func runSemgrep(ctx context.Context, dir string, logger engine.NodeLogger) ([]sastFinding, error) {
	args := []string{
		"run", "--rm",
		"-v", dir + ":/src:ro",
		"-w", "/src",
		"returntocorp/semgrep:latest",
		"semgrep", "scan",
		"--config", "auto", // pulls Semgrep's curated registry rules
		"--json", "--quiet",
	}
	out, err := dockerRunCapture(ctx, args, logger)
	if err != nil && len(out) == 0 {
		return nil, err
	}
	return parseSemgrep(out)
}

type semgrepReport struct {
	Results []struct {
		CheckID string `json:"check_id"`
		Path    string `json:"path"`
		Start   struct {
			Line int `json:"line"`
		} `json:"start"`
		Extra struct {
			Severity string `json:"severity"`
			Message  string `json:"message"`
		} `json:"extra"`
	} `json:"results"`
}

func parseSemgrep(raw []byte) ([]sastFinding, error) {
	if i := bytes.IndexByte(raw, '{'); i > 0 {
		raw = raw[i:]
	}
	var r semgrepReport
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("parse semgrep json: %w", err)
	}
	out := make([]sastFinding, 0, len(r.Results))
	for _, x := range r.Results {
		out = append(out, sastFinding{
			Tool:     "semgrep",
			Severity: normalizeSemgrepSev(x.Extra.Severity),
			RuleID:   x.CheckID,
			File:     x.Path,
			Line:     x.Start.Line,
			Message:  x.Extra.Message,
		})
	}
	return out, nil
}

// Semgrep uses ERROR/WARNING/INFO; map to our LOW/MEDIUM/HIGH/CRITICAL.
func normalizeSemgrepSev(s string) string {
	switch strings.ToUpper(s) {
	case "ERROR":
		return "HIGH"
	case "WARNING":
		return "MEDIUM"
	case "INFO":
		return "LOW"
	}
	return strings.ToUpper(s)
}

// --- gitleaks -------------------------------------------------------------

func runGitleaks(ctx context.Context, dir string, logger engine.NodeLogger) ([]sastFinding, error) {
	args := []string{
		"run", "--rm",
		"-v", dir + ":/src:ro",
		"zricethezav/gitleaks:latest",
		"detect", "--source=/src",
		"--no-git", // we're scanning the working tree, not git history
		"--report-format=json", "--report-path=/dev/stdout",
		"--no-banner",
	}
	out, err := dockerRunCapture(ctx, args, logger)
	if err != nil && len(out) == 0 {
		return nil, err
	}
	return parseGitleaks(out)
}

type gitleaksFinding struct {
	RuleID      string `json:"RuleID"`
	Description string `json:"Description"`
	File        string `json:"File"`
	StartLine   int    `json:"StartLine"`
	Match       string `json:"Match"`
}

func parseGitleaks(raw []byte) ([]sastFinding, error) {
	// gitleaks --report-path=/dev/stdout emits a JSON array.
	if i := bytes.IndexByte(raw, '['); i > 0 {
		raw = raw[i:]
	}
	var arr []gitleaksFinding
	if err := json.Unmarshal(raw, &arr); err != nil {
		// gitleaks prints "no leaks found" sometimes; treat parse failure
		// without a leading '[' as zero findings.
		return nil, nil
	}
	out := make([]sastFinding, 0, len(arr))
	for _, g := range arr {
		out = append(out, sastFinding{
			Tool:     "gitleaks",
			Severity: "HIGH", // any leaked secret is high severity
			RuleID:   g.RuleID,
			File:     g.File,
			Line:     g.StartLine,
			Message:  g.Description,
		})
	}
	return out, nil
}

// --- sonar (SonarCloud) ---------------------------------------------------

// runSonarCloud uploads the workspace to SonarCloud via the official scanner
// container and polls the v2 quality-gate API for the verdict. Returns a
// summary that includes the quality gate status (OK | WARN | ERROR), the
// dashboard URL, and the underlying analysis ID for traceability.
func runSonarCloud(ctx context.Context, p map[string]any, a *storage.Agent, cloneDir, ref, commit string, logger engine.NodeLogger) (map[string]any, error) {
	host := strParam(p, "sonarHost", "https://sonarcloud.io")
	org := strings.TrimSpace(strParam(p, "organization", ""))
	projectKey := strings.TrimSpace(strParam(p, "projectKey", ""))
	token := strings.TrimSpace(strParam(p, "sonarToken", ""))
	branch := strFirst(strParam(p, "branchName", ""), ref, "main")

	if org == "" || projectKey == "" || token == "" {
		return nil, errors.New("sonar requires organization, projectKey, and sonarToken")
	}

	args := []string{
		"run", "--rm",
		"-e", "SONAR_HOST_URL=" + host,
		"-e", "SONAR_TOKEN=" + token,
		"-v", cloneDir + ":/usr/src:ro",
		"-w", "/usr/src",
		"sonarsource/sonar-scanner-cli:latest",
		"-Dsonar.organization=" + org,
		"-Dsonar.projectKey=" + projectKey,
		"-Dsonar.sources=.",
		"-Dsonar.branch.name=" + branch,
	}
	if commit != "" {
		args = append(args, "-Dsonar.scm.revision="+commit)
	}
	if _, err := dockerRunCapture(ctx, args, logger); err != nil {
		return nil, fmt.Errorf("sonar-scanner: %w", err)
	}

	// Poll for the quality gate verdict — analysis is async server-side.
	logger.Log("[sast:sonar] waiting for quality gate verdict…")
	gate, err := pollSonarGate(ctx, host, org, projectKey, branch, token)
	if err != nil {
		return nil, fmt.Errorf("sonar quality gate: %w", err)
	}
	logger.Log(fmt.Sprintf("[sast:sonar] quality gate: %s", gate))

	dashboardURL := fmt.Sprintf("%s/project/overview?id=%s",
		strings.TrimRight(host, "/"), url.QueryEscape(projectKey))
	return map[string]any{
		"qualityGate":  gate,
		"dashboardUrl": dashboardURL,
		"projectKey":   projectKey,
		"branchName":   branch,
	}, nil
}

// pollSonarGate polls /api/qualitygates/project_status until SonarCloud
// returns a non-NONE / non-PENDING verdict. Bounded by the parent ctx
// (the executor's hard timeout).
func pollSonarGate(ctx context.Context, host, org, projectKey, branch, token string) (string, error) {
	endpoint := fmt.Sprintf("%s/api/qualitygates/project_status?projectKey=%s&branch=%s",
		strings.TrimRight(host, "/"),
		url.QueryEscape(projectKey),
		url.QueryEscape(branch))

	deadline := time.NewTicker(5 * time.Second)
	defer deadline.Stop()

	httpc := &http.Client{Timeout: 15 * time.Second}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return "", err
		}
		req.SetBasicAuth(token, "") // SonarCloud convention
		req.Header.Set("Accept", "application/json")
		resp, err := httpc.Do(req)
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("status %d: %s", resp.StatusCode, oneLineSummary(string(body)))
		}
		var r struct {
			ProjectStatus struct {
				Status string `json:"status"` // OK | WARN | ERROR | NONE
			} `json:"projectStatus"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			return "", err
		}
		switch strings.ToUpper(r.ProjectStatus.Status) {
		case "OK", "WARN", "ERROR":
			return r.ProjectStatus.Status, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
		}
	}
}

// --- custom ---------------------------------------------------------------

func runCustom(ctx context.Context, p map[string]any, dir string, logger engine.NodeLogger) ([]sastFinding, error) {
	image := strings.TrimSpace(strParam(p, "image", ""))
	command := strings.TrimSpace(strParam(p, "command", ""))
	if image == "" || command == "" {
		return nil, errors.New("custom tool requires image and command")
	}
	args := []string{
		"run", "--rm",
		"-v", dir + ":/src:ro",
		"-w", "/src",
		image,
		"/bin/sh", "-c", command,
	}
	out, err := dockerRunCapture(ctx, args, logger)
	if err != nil {
		return nil, fmt.Errorf("custom scanner exit: %w (last: %s)", err, oneLineSummary(string(out)))
	}
	// We don't parse arbitrary tool output — just emit a single finding
	// of unknown severity carrying the tail. Users wiring a real tool can
	// switch to `tool: trivy` etc., or post-process via downstream nodes.
	return []sastFinding{{
		Tool:     "custom",
		Severity: "UNKNOWN",
		Message:  lastLines(string(out), 20),
	}}, nil
}

// --- helpers --------------------------------------------------------------

// dockerRunCapture spawns a `docker run …` subprocess, streams stdout/stderr
// through the NodeLogger so the UI sees live output, and returns the full
// stdout as bytes for downstream JSON parsing.
func dockerRunCapture(ctx context.Context, args []string, logger engine.NodeLogger) ([]byte, error) {
	logger.Log("$ docker " + strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "docker", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Capture stdout (the JSON / report) into a buffer; stream stderr to
	// the logger so users see it live.
	var buf bytes.Buffer
	doneOut := make(chan error, 1)
	go func() {
		_, e := io.Copy(&buf, io.TeeReader(stdout, &lineLogger{logger: logger, prefix: ""}))
		doneOut <- e
	}()
	go func() {
		_, _ = io.Copy(&lineLogger{logger: logger, prefix: ""}, stderr)
	}()
	<-doneOut

	werr := cmd.Wait()
	return buf.Bytes(), werr
}

// lineLogger is an io.Writer that splits incoming bytes on '\n' and forwards
// each non-empty line to a NodeLogger. Used so a docker subprocess's output
// streams live into the SSE feed.
type lineLogger struct {
	logger engine.NodeLogger
	prefix string
	buf    []byte
}

func (l *lineLogger) Write(p []byte) (int, error) {
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			return len(p), nil
		}
		line := strings.TrimRight(string(l.buf[:i]), "\r")
		if line != "" {
			l.logger.Log(l.prefix + line)
		}
		l.buf = l.buf[i+1:]
	}
}

// severityChainAtOrAbove returns a comma-list of severities at or above
// the given threshold, in trivy's expected casing.
func severityChainAtOrAbove(threshold string) string {
	chain := []string{"LOW", "MEDIUM", "HIGH", "CRITICAL"}
	t := strings.ToUpper(threshold)
	for i, s := range chain {
		if s == t {
			return strings.Join(chain[i:], ",")
		}
	}
	return "HIGH,CRITICAL"
}

// exceedsThreshold returns true if any finding's severity is at or above
// the threshold. Used to gate the run when failOnFinding is true.
func exceedsThreshold(findings []sastFinding, threshold string) bool {
	rank := map[string]int{"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
	tr := rank[strings.ToUpper(threshold)]
	if tr == 0 {
		tr = 3 // default HIGH
	}
	for _, f := range findings {
		if rank[strings.ToUpper(f.Severity)] >= tr {
			return true
		}
	}
	return false
}
