package main

// Git: per-job repository/branch/credentials (UI) with env defaults (GIT_REPO_URL, GIT_TOKEN).
// The clone is persistent per job inside the pod, shallow + partial (blob:none) + sparse (only the
// job's folder), so a run transfers just the new commit. HTTPS uses a token in an
// Authorization header from the environment; SSH uses a private key written to a 0600 temp file.

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type gitTarget struct {
	url, branch, user, auth, secret string
}

func resolveGit(cfg *Settings, g GitConfig) (gitTarget, error) {
	t := gitTarget{url: strings.TrimSpace(g.RepoURL), branch: strings.TrimSpace(g.Branch),
		user: strings.TrimSpace(g.Username), auth: g.Auth}
	if t.url == "" {
		t.url = cfg.GitRepoURL
		t.secret = os.Getenv("GIT_TOKEN")
		if t.user == "" {
			t.user = os.Getenv("GIT_USERNAME")
		}
	} else {
		s, err := decryptSecret(g.SecretEnc)
		if err != nil {
			return t, err
		}
		t.secret = s
	}
	if t.url == "" {
		return t, fmt.Errorf("no repository set in the sync or in GIT_REPO_URL")
	}
	if t.branch == "" {
		t.branch = cfg.GitBranch
	}
	if t.user == "" {
		t.user = "git"
	}
	if t.auth == "" {
		t.auth = "token"
		if strings.HasPrefix(t.url, "git@") || strings.HasPrefix(t.url, "ssh://") {
			t.auth = "ssh"
		}
	}
	return t, nil
}

// gitEnv returns environment for git and a cleanup func (removes the temporary SSH key).
func gitEnv(workDir string, t gitTarget) ([]string, func(), error) {
	env := []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=" + env("GIT_AUTHOR_NAME", "kamin-db-syncer"),
		"GIT_AUTHOR_EMAIL=" + env("GIT_AUTHOR_EMAIL", "kamin-db-syncer@localhost"),
		"GIT_COMMITTER_NAME=" + env("GIT_AUTHOR_NAME", "kamin-db-syncer"),
		"GIT_COMMITTER_EMAIL=" + env("GIT_AUTHOR_EMAIL", "kamin-db-syncer@localhost"),
	}
	cleanup := func() {}
	switch {
	case t.secret == "":
	case t.auth == "ssh":
		f, err := os.CreateTemp(workDir, "sshkey-*")
		if err != nil {
			return nil, cleanup, err
		}
		key := strings.ReplaceAll(t.secret, "\r\n", "\n")
		if !strings.HasSuffix(key, "\n") {
			key += "\n"
		}
		_, werr := f.WriteString(key)
		f.Close()
		cleanup = func() { os.Remove(f.Name()) }
		if werr != nil {
			cleanup()
			return nil, func() {}, werr
		}
		if err := os.Chmod(f.Name(), 0o600); err != nil {
			cleanup()
			return nil, func() {}, err
		}
		known := filepath.Join(workDir, "known_hosts")
		env = append(env, fmt.Sprintf("GIT_SSH_COMMAND=ssh -i %s -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=%s", f.Name(), known))
	default: // token in a header from env, never in the URL or .git/config
		basic := base64.StdEncoding.EncodeToString([]byte(t.user + ":" + t.secret))
		env = append(env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0=Authorization: Basic "+basic)
	}
	return env, cleanup, nil
}

// testGit checks access and whether the branch exists (ls-remote: no clone, a few hundred bytes).
func testGit(ctx context.Context, cfg *Settings, g GitConfig) (string, error) {
	t, err := resolveGit(cfg, g)
	if err != nil {
		return "", err
	}
	env, cleanup, err := gitEnv(cfg.WorkDir, t)
	if err != nil {
		return "", err
	}
	defer cleanup()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := gitOutput(ctx, env, cfg.WorkDir, "ls-remote", "--heads", t.url, t.branch)
	if err != nil {
		return "", fmt.Errorf("no access to %s: %v", t.url, err)
	}
	if strings.TrimSpace(out) == "" {
		return fmt.Sprintf("access OK; branch %s does not exist yet — the first commit will create it", t.branch), nil
	}
	return fmt.Sprintf("access OK, branch %s found", t.branch), nil
}

func (rc *runCtx) gitSync(snapshot string) (bool, error) {
	cfg := rc.cfg
	t, err := resolveGit(cfg, rc.job.Config.Git)
	if err != nil {
		return false, err
	}
	sub := rc.job.Config.Git.Path
	if sub == "" {
		sub = rc.job.Name
	}
	sub = filepath.ToSlash(filepath.Clean(sub))
	if strings.HasPrefix(sub, "..") || filepath.IsAbs(sub) {
		return false, fmt.Errorf("invalid folder %q", sub)
	}
	genv, cleanup, err := gitEnv(cfg.WorkDir, t)
	if err != nil {
		return false, err
	}
	defer cleanup()

	repo := filepath.Join(cfg.WorkDir, "git", rc.job.Name) // survives between runs of this pod
	git := func(args ...string) error { return rc.runCmd("git: ", genv, repo, "git", args...) }

	// a changed repository URL invalidates the cached clone
	if _, err := os.Stat(filepath.Join(repo, ".git")); err == nil {
		if u, _ := gitOutput(rc.ctx, genv, repo, "remote", "get-url", "origin"); strings.TrimSpace(u) != t.url {
			_ = os.RemoveAll(repo)
		}
	}
	heads, err := gitOutput(rc.ctx, genv, cfg.WorkDir, "ls-remote", "--heads", t.url, t.branch)
	if err != nil {
		return false, fmt.Errorf("no access to repository %s: %v", t.url, err)
	}
	remoteHasBranch := strings.TrimSpace(heads) != ""

	if _, err := os.Stat(filepath.Join(repo, ".git")); err != nil {
		_ = os.RemoveAll(repo)
		if err := os.MkdirAll(repo, 0o755); err != nil {
			return false, err
		}
		if remoteHasBranch {
			rc.log.Printf("git: initial clone of %s (shallow, partial, folder %s only)", t.url, sub)
			if err := git("clone", "--quiet", "--depth", "1", "--filter=blob:none", "--no-checkout",
				"--branch", t.branch, t.url, "."); err != nil {
				_ = os.RemoveAll(repo)
				return false, err
			}
			if err := git("sparse-checkout", "set", "--no-cone", "/"+sub+"/"); err != nil {
				return false, err
			}
		} else {
			rc.log.Printf("git: branch %s not found in %s — creating it", t.branch, t.url)
			if err := git("init", "--quiet", "-b", t.branch); err != nil {
				return false, err
			}
			if err := git("remote", "add", "origin", t.url); err != nil {
				return false, err
			}
		}
	}

	msg := fmt.Sprintf("[%s] schema snapshot %s", rc.job.Name, time.Now().In(cfg.TZ).Format("2006-01-02 15:04"))
	for attempt := 1; attempt <= 5; attempt++ {
		// always rebuild the commit on top of the newest remote tip: another pod may have pushed
		if remoteHasBranch {
			if err := git("fetch", "--quiet", "--depth", "1", "origin", t.branch); err != nil {
				return false, err
			}
			if err := git("reset", "--quiet", "--hard", "FETCH_HEAD"); err != nil {
				return false, err
			}
		}
		dst := filepath.Join(repo, filepath.FromSlash(sub))
		if err := os.RemoveAll(dst); err != nil { // full replace: dropped objects disappear from git
			return false, err
		}
		if err := copyDir(snapshot, dst); err != nil {
			return false, err
		}
		if err := git("add", "--sparse", "-A", "--", sub); err != nil {
			return false, err
		}
		if remoteHasBranch && rc.runCmd("", genv, repo, "git", "diff", "--cached", "--quiet") == nil {
			rc.log.Printf("git: no changes")
			return false, nil
		}
		if err := git("commit", "--quiet", "-m", msg); err != nil {
			return false, err
		}
		if err := git("push", "--quiet", "origin", "HEAD:"+t.branch); err == nil {
			rc.recordSchemaChange(genv, repo, sub, !remoteHasBranch || rc.job.State.GitHash == "")
			return true, nil
		}
		if rc.ctx.Err() != nil {
			return false, rc.ctx.Err()
		}
		rc.log.Printf("git: push rejected, attempt %d/5", attempt)
		remoteHasBranch = true
		time.Sleep(time.Duration(attempt*2) * time.Second)
	}
	return false, fmt.Errorf("push failed after 5 attempts")
}

func (rc *runCtx) recordSchemaChange(genv []string, repo, sub string, baseline bool) {
	var c *SchemaChange
	var err error
	if _, perr := gitOutput(rc.ctx, genv, repo, "rev-parse", "--verify", "--quiet", "HEAD~1"); perr != nil {
		// first commit of a new branch: everything is new
		c, err = describeRoot(rc.ctx, genv, repo, sub)
	} else {
		c, err = describeCommit(rc.ctx, genv, repo, sub, baseline)
	}
	if err != nil {
		rc.log.Printf("git: could not describe the changes: %v", err)
		return
	}
	c.JobID, c.JobName, c.RunID = rc.job.ID, rc.job.Name, rc.runID
	rc.log.Printf("git: pushed %s — %d added, %d modified, %d deleted", c.Commit[:min(10, len(c.Commit))], c.Added, c.Modified, c.Deleted)
	if !c.Baseline {
		for _, ch := range c.Changes {
			rc.log.Printf("  schema %s %s %s", ch.Status, ch.Kind, ch.Object)
		}
	}
	rc.notes = append(rc.notes, fmt.Sprintf("schema in git: +%d ~%d −%d", c.Added, c.Modified, c.Deleted))
	if err := rc.saveSchemaChange(c); err != nil {
		rc.log.Printf("could not save the schema history: %v", err)
	}
}

func describeRoot(ctx context.Context, env []string, repo, sub string) (*SchemaChange, error) {
	sha, err := gitOutput(ctx, env, repo, "rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	files, err := gitOutput(ctx, env, repo, "ls-tree", "-r", "--name-only", "HEAD", "--", sub)
	if err != nil {
		return nil, err
	}
	c := &SchemaChange{Commit: strings.TrimSpace(sha), Baseline: true, At: utcNow()}
	for _, f := range strings.Split(strings.TrimSpace(files), "\n") {
		if f == "" {
			continue
		}
		oc := ObjChange{Status: "A", Path: strings.TrimPrefix(f, sub+"/")}
		oc.Kind, oc.Object = objectOf(oc.Path)
		c.Changes = append(c.Changes, oc)
		c.Added++
	}
	return c, nil
}
