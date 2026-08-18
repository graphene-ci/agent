package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/graphene-ci/agent/pkg/host"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Store is the agent's durable memory of the containers it was asked to
// host: one JSON file per container under dataDir/state. Detached
// containers survive an agent restart — the store lets the restarted
// agent find and keep supervising them.
type Store struct {
	dir string

	mu    sync.Mutex
	items map[string]host.RunContainer
}

// OpenStore loads the persisted container records.
func OpenStore(dataDir string) (*Store, error) {
	dir := filepath.Join(dataDir, "state")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, items: map[string]host.RunContainer{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // the agent's own state dir
		if err != nil {
			return nil, err
		}
		var c host.RunContainer
		if err := json.Unmarshal(raw, &c); err != nil {
			// A corrupt record is dropped, not fatal: the container (if
			// any) keeps running and shows up unowned in reports.
			continue
		}
		s.items[key(c.AgentId, c.RunId)] = c
	}
	return s, nil
}

// Put records a container.
func (s *Store) Put(c host.RunContainer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key(c.AgentId, c.RunId)] = c
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(s.file(key(c.AgentId, c.RunId)), raw, 0o600)
}

// Delete forgets a container.
func (s *Store) Delete(c host.RunContainer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key(c.AgentId, c.RunId))
	err := os.Remove(s.file(key(c.AgentId, c.RunId)))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// List returns the known containers in stable order.
func (s *Store) List() []host.RunContainer {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]host.RunContainer, 0, len(s.items))
	for _, c := range s.items {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		return key(out[i].AgentId, out[i].RunId) < key(out[j].AgentId, out[j].RunId)
	})
	return out
}

// Get finds one container record.
func (s *Store) Get(agentId id.AgentId, runId id.RunId) (host.RunContainer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.items[key(agentId, runId)]
	return c, ok
}

func (s *Store) file(k string) string {
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			return r
		default:
			return '_'
		}
	}, k)
	return filepath.Join(s.dir, sanitized+".json")
}

func key[M ~string, R ~string](m M, r R) string {
	return string(m) + "\x00" + string(r)
}
