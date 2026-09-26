package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/sllt/pi/pkg/pi/version"
)

const repoURL = "https://github.com/sllt/pi-layout.git"
const oldPackageName = "github.com/sllt/pi-layout"

var ErrNameEmpty = errors.New("please provide the project directory")
var ErrProjectExists = errors.New("project directory already exists")

// Options separates the filesystem destination from the Go module identity.
// Offline requires a local Git template and skips dependency/build verification.
type Options struct {
	Directory string
	Module    string
	Ref       string
	Template  string
	Offline   bool
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
	if o.Offline {
		info, err := os.Stat(o.Template)
		if err != nil || !info.IsDir() {
			return errors.New("offline mode requires a local Git template directory")
		}
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
	if _, err = command(ctx, "", "git", "clone", "--no-checkout", "--", o.Template, project); err != nil {
		return err
	}
	if _, err = command(ctx, project, "git", "checkout", "--detach", o.Ref); err != nil {
		return err
	}
	commit, err := command(ctx, project, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if err = os.RemoveAll(filepath.Join(project, ".git")); err != nil {
		return err
	}
	for _, name := range []string{"configs/.env", "go.work", "go.work.sum"} {
		if err = os.Remove(filepath.Join(project, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	err = filepath.WalkDir(project, func(name string, d os.DirEntry, walkErr error) error {
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
			data, err = rewriteDescriptor(data, o.Module)
			if err != nil {
				return fmt.Errorf("protobuf metadata %s: %w", name, err)
			}
		}
		data = bytes.ReplaceAll(data, []byte(oldPackageName), []byte(o.Module))
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
	if err != nil {
		return err
	}
	if _, err = command(ctx, project, "go", "mod", "edit", "-module", o.Module); err != nil {
		return err
	}
	if !o.Offline {
		if _, err = command(ctx, project, "go", "build", "-mod=readonly", "./..."); err != nil {
			return fmt.Errorf("generated application verification failed: %w", err)
		}
	}
	mod, err := os.ReadFile(filepath.Join(project, "go.mod"))
	if err != nil {
		return err
	}
	framework := regexp.MustCompile(`github.com/sllt/pi\s+(\S+)`).FindSubmatch(mod)
	if len(framework) != 2 {
		return errors.New("template is missing the Pi framework dependency")
	}
	meta, _ := json.MarshalIndent(map[string]any{"cli": version.Framework, "framework": string(framework[1]), "template_ref": o.Ref, "template_commit": strings.TrimSpace(commit), "verified": !o.Offline}, "", "  ")
	if err = os.WriteFile(filepath.Join(project, ".pi-template.json"), append(meta, '\n'), 0644); err != nil {
		return err
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
	if o.Offline {
		fmt.Printf("Created %s (offline, build NOT verified)\n", dest)
	} else {
		fmt.Printf("Created and verified %s\n", dest)
	}
	return nil
}

func command(ctx context.Context, dir, program string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %w: %s", program, err, out)
	}
	return string(out), nil
}
