package helper

import (
	"errors"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
)

// File is fully rendered before delivery. Replace must be explicitly enabled
// only for generated files; handwritten skeletons are never overwritten.
type File struct {
	Data    []byte
	Replace bool
}

// WriteFiles formats every file before writing, stages beside each destination,
// and restores originals if delivery fails. Each rename is atomic; the set is
// not a filesystem transaction (SIGKILL may require rerunning the generator).
func WriteFiles(files map[string]File) (err error) {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	staged := map[string]string{}
	original := map[string][]byte{}
	delivered := []string{}
	defer func() {
		if err != nil {
			for i := len(delivered) - 1; i >= 0; i-- {
				name := delivered[i]
				if data, exists := original[name]; exists {
					err = errors.Join(err, os.WriteFile(name, data, 0644))
				} else {
					err = errors.Join(err, os.Remove(name))
				}
			}
		}
		for _, name := range staged {
			_ = os.Remove(name)
		}
	}()
	for _, name := range names {
		f := files[name]
		data, e := format.Source(f.Data)
		if e != nil {
			return fmt.Errorf("format %s: %w", name, e)
		}
		old, e := os.ReadFile(name)
		if e == nil {
			if !f.Replace {
				return fmt.Errorf("file already exists: %s", name)
			}
			original[name] = old
		} else if !os.IsNotExist(e) {
			return e
		}
		if e = os.MkdirAll(filepath.Dir(name), 0755); e != nil {
			return e
		}
		tmp, e := os.CreateTemp(filepath.Dir(name), ".pi-generate-*")
		if e != nil {
			return e
		}
		staged[name] = tmp.Name()
		_, e = tmp.Write(data)
		e = errors.Join(e, tmp.Chmod(0644), tmp.Close())
		if e != nil {
			return e
		}
	}
	for _, name := range names {
		if !files[name].Replace {
			// Hard-link provides atomic create-if-absent semantics.
			if err = os.Link(staged[name], name); err != nil {
				return err
			}
		} else if err = os.Rename(staged[name], name); err != nil {
			return err
		}
		delivered = append(delivered, name)
	}
	return nil
}
