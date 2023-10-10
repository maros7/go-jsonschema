package schemas

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/atombender/go-jsonschema/pkg/yamlutils"
	"github.com/goccy/go-yaml"
)

var errInvalidSchemaRef = fmt.Errorf("schema reference must a file name or HTTP URL")

func FromJSONFile(fileName string) (*Schema, error) {
	var l Loader
	r, err := l.Load(fileName)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = r.Close()
	}()

	return FromJSONReader(r)
}

func FromJSONReader(r io.Reader) (*Schema, error) {
	var schema Schema
	if err := json.NewDecoder(r).Decode(&schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return &schema, nil
}

func FromYAMLFile(fileName string) (*Schema, error) {
	var l Loader
	r, err := l.Load(fileName)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = r.Close()
	}()

	return FromYAMLReader(r)
}

func FromYAMLReader(r io.Reader) (*Schema, error) {
	// Marshal to JSON first because YAML decoder doesn't understand JSON tags.
	var m map[string]interface{}

	if err := yaml.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	yamlutils.FixMapKeys(m)

	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	var schema Schema

	if err = json.Unmarshal(b, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return &schema, nil
}

type Loader struct {
	workingDir string
}

func (l *Loader) Load(fileName string) (io.ReadCloser, error) {
	u, err := url.Parse(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}

	if u.Scheme == "http" || u.Scheme == "https" {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, fileName, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to do request: %w", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, fmt.Errorf("failed to do request: %d", resp.StatusCode)
		}

		return resp.Body, nil
	}

	if (u.Scheme == "" || u.Scheme == "file") && u.Host == "" && u.Path != "" {
		rc, err := os.Open(filepath.Join(l.workingDir, u.Path))
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}

		return rc, nil
	}

	return nil, fmt.Errorf("%w: %q", errInvalidSchemaRef, fileName)
}

func (l *Loader) FromJSONFile(fileName string) (*Schema, error) {
	r, err := l.Load(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to load: %w", err)
	}

	defer func() {
		_ = r.Close()
	}()

	return FromJSONReader(r)
}

func (l *Loader) FromJSONReader(r io.Reader) (*Schema, error) {
	var schema Schema
	if err := json.NewDecoder(r).Decode(&schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return &schema, nil
}

func (l *Loader) FromYAMLFile(fileName string) (*Schema, error) {
	r, err := l.Load(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to load: %w", err)
	}

	defer func() {
		_ = r.Close()
	}()

	return FromYAMLReader(r)
}

func (l *Loader) FromYAMLReader(r io.Reader) (*Schema, error) {
	// Marshal to JSON first because YAML decoder doesn't understand JSON tags.
	var m map[string]interface{}

	if err := yaml.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	yamlutils.FixMapKeys(m)

	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	var schema Schema

	if err = json.Unmarshal(b, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return &schema, nil
}
