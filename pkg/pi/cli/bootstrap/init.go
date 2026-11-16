package bootstrap

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi/version"
)

const repoURL = "https://github.com/sllt/pi-layout.git"
const oldPackageName = "github.com/sllt/pi-layout"

var ErrNameEmpty = errors.New("please provide the project directory")
var ErrProjectExists = errors.New("project directory already exists")
var hexCommitPattern = regexp.MustCompile("^[0-9a-fA-F]{4,64}$")

// Options separates the filesystem destination from the Go module identity.
// Offline requires a local Git template directory and skips dependency/build verification.
// SkipVerify skips initial Go dependency download and build verification for both remote and local templates.
// Out overrides the progress and summary output writer (defaults to os.Stdout when nil).
type Options struct {
	Directory  string
	Module     string
	Ref        string
	Template   string
	Offline    bool
	SkipVerify bool
	Out        io.Writer
}

func Create(directory string) error {
	return CreateContext(context.Background(), Options{Directory: directory})
}

func CreateContext(ctx context.Context, o Options) error {
	if o.Directory == "" {
		return ErrNameEmpty
	}
	dest, err := filepath.Abs(o.Directory)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(dest); !os.IsNotExist(err) {
		return ErrProjectExists
	}
	if o.Module == "" {
		o.Module = filepath.Base(dest)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~+-]*(/[A-Za-z0-9][A-Za-z0-9._~+-]*)*$`).MatchString(o.Module) || strings.Contains(o.Module, "..") {
		return fmt.Errorf("invalid Go module path %q", o.Module)
	}
	if o.Ref == "" {
		o.Ref = version.Framework
	}
	if strings.HasPrefix(o.Ref, "-") {
		return errors.New("template ref cannot start with '-' ")
	}
	if o.Template == "" {
		o.Template = repoURL
	}
	if o.Offline && !isLocalDir(o.Template) {
		return errors.New("offline mode requires a local Git template directory")
	}
	out := o.Out
	if out == nil {
		out = os.Stdout
	}
	verify := !o.Offline && !o.SkipVerify
	totalSteps := 4
	if !verify {
		totalSteps = 2
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(filepath.Dir(dest), ".pi-init-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	project := filepath.Join(stage, "project")

	var commit string
	if err := runStep(out, 1, totalSteps, fmt.Sprintf("Fetching template %s (%s)", o.Template, o.Ref), func() error {
		c, err := cloneTemplate(ctx, o.Template, o.Ref, project)
		if err != nil {
			return err
		}
		commit = c
		if err := os.RemoveAll(filepath.Join(project, ".git")); err != nil {
			return err
		}
		for _, name := range []string{"configs/.env", "go.work", "go.work.sum"} {
			if err := os.Remove(filepath.Join(project, name)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	var envCreated bool
	if err := runStep(out, 2, totalSteps, fmt.Sprintf("Rewriting module path to %s", o.Module), func() error {
		if err := rewriteTemplateFiles(project, o.Module); err != nil {
			return err
		}
		if _, err := command(ctx, project, "go", "mod", "edit", "-module", o.Module); err != nil {
			return err
		}
		created, err := initLocalEnvFile(project)
		if err != nil {
			return err
		}
		envCreated = created
		mod, err := os.ReadFile(filepath.Join(project, "go.mod"))
		if err != nil {
			return err
		}
		framework := regexp.MustCompile(`github.com/sllt/pi\s+(\S+)`).FindSubmatch(mod)
		if len(framework) != 2 {
			return errors.New("template is missing the Pi framework dependency")
		}
		meta, _ := json.MarshalIndent(map[string]any{
			"cli":             version.Framework,
			"framework":       string(framework[1]),
			"template_ref":    o.Ref,
			"template_commit": strings.TrimSpace(commit),
			"verified":        verify,
		}, "", "  ")
		return os.WriteFile(filepath.Join(project, ".pi-template.json"), append(meta, '\n'), 0644)
	}); err != nil {
		return err
	}

	if verify {
		if err := runStep(out, 3, totalSteps, "Downloading Go module dependencies", func() error {
			if _, err := command(ctx, project, "go", "mod", "download"); err != nil {
				return fmt.Errorf("download module dependencies failed: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := runStep(out, 4, totalSteps, "Verifying project build (pass --skip-verify to skip)", func() error {
			if _, err := command(ctx, project, "go", "build", "-mod=readonly", "./..."); err != nil {
				return fmt.Errorf("generated application verification failed: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if err = ctx.Err(); err != nil {
		return err
	}
	lock, err := os.OpenFile(dest+".pi-init.lock", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	lock.Close()
	defer os.Remove(dest + ".pi-init.lock")
	if _, err = os.Lstat(dest); !os.IsNotExist(err) {
		return ErrProjectExists
	}
	if err = os.Rename(project, dest); err != nil {
		return err
	}
	switch {
	case o.Offline:
		fmt.Fprintf(out, "Created %s (offline, build NOT verified)\n", dest)
	case o.SkipVerify:
		fmt.Fprintf(out, "Created %s (build verification skipped via --skip-verify)\n", dest)
	default:
		fmt.Fprintf(out, "Created and verified %s\n", dest)
	}
	if envCreated {
		fmt.Fprintf(out, "Generated local %s with a random JWT_SECRET.\n", filepath.Join(dest, "configs", ".env"))
		fmt.Fprintf(out, "Next steps:\n  cd %s\n  go run ./cmd/migration up\n  go run ./cmd/server\n", o.Directory)
	}
	return nil
}

func isLocalDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func canTryShallowBranchClone(ref string) bool {
	return ref != "" &&
		ref != "HEAD" &&
		!strings.HasPrefix(ref, "refs/") &&
		!strings.ContainsAny(ref, "~^:@") &&
		!hexCommitPattern.MatchString(ref)
}

func cloneTemplate(ctx context.Context, template, ref, project string) (string, error) {
	if canTryShallowBranchClone(ref) {
		args := []string{"clone", "-c", "advice.detachedHead=false"}
		if !isLocalDir(template) {
			args = append(args, "--depth", "1")
		}
		args = append(args, "--branch", ref, "--single-branch", "--", template, project)
		if _, err := command(ctx, "", "git", args...); err == nil {
			commit, err := command(ctx, project, "git", "rev-parse", "HEAD")
			if err != nil {
				return "", err
			}
			return strings.TrimSpace(commit), nil
		} else if ctx.Err() != nil {
			return "", err
		}
		_ = os.RemoveAll(project)
	}

	if _, err := command(ctx, "", "git", "clone", "--no-checkout", "--", template, project); err != nil {
		return "", err
	}
	if _, err := command(ctx, project, "git", "-c", "advice.detachedHead=false", "checkout", "--detach", ref); err != nil {
		return "", err
	}
	commit, err := command(ctx, project, "git", "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(commit), nil
}

func rewriteTemplateFiles(project, module string) error {
	return filepath.WalkDir(project, func(name string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("template symlink is not supported: %s", name)
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		if bytes.IndexByte(data, 0) >= 0 {
			return nil
		}
		if strings.HasSuffix(name, ".pb.go") {
			data, err = rewriteDescriptor(data, module)
			if err != nil {
				return fmt.Errorf("protobuf metadata %s: %w", name, err)
			}
		}
		data = bytes.ReplaceAll(data, []byte(oldPackageName), []byte(module))
		if filepath.Ext(name) == ".go" {
			data, err = format.Source(data)
			if err != nil {
				return fmt.Errorf("format %s: %w", name, err)
			}
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.WriteFile(name, data, info.Mode().Perm())
	})
}

func runStep(out io.Writer, step, total int, label string, fn func() error) error {
	return runStepWithInterval(out, step, total, label, 2*time.Second, fn)
}

func runStepWithInterval(out io.Writer, step, total int, label string, interval time.Duration, fn func() error) error {
	if out == nil {
		out = os.Stdout
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	var mu sync.Mutex
	start := time.Now()
	mu.Lock()
	fmt.Fprintf(out, "[%d/%d] %s...", step, total, label)
	mu.Unlock()

	done := make(chan struct{})
	stopped := make(chan struct{})
	ticker := time.NewTicker(interval)
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				mu.Lock()
				fmt.Fprint(out, ".")
				mu.Unlock()
			}
		}
	}()

	err := fn()
	close(done)
	<-stopped

	elapsed := time.Since(start).Seconds()
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		fmt.Fprintf(out, " failed (%.1fs)\n", elapsed)
		return err
	}
	fmt.Fprintf(out, " done (%.1fs)\n", elapsed)
	return nil
}

var jwtSecretLinePattern = regexp.MustCompile(`(?m)^JWT_SECRET=.*$`)

func initLocalEnvFile(projectDir string) (bool, error) {
	examplePath := filepath.Join(projectDir, "configs", ".env.example")
	data, err := os.ReadFile(examplePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	var randomBytes [32]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return false, fmt.Errorf("generate JWT_SECRET: %w", err)
	}
	secretHex := hex.EncodeToString(randomBytes[:])
	rendered := jwtSecretLinePattern.ReplaceAll(data, []byte("JWT_SECRET="+secretHex))

	envPath := filepath.Join(projectDir, "configs", ".env")
	if err := os.WriteFile(envPath, rendered, 0600); err != nil {
		return false, err
	}
	return true, nil
}

func command(ctx context.Context, dir, program string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %w: %s", program, err, out)
	}
	return string(out), nil
}
