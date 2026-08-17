package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

var (
	metaBucket        = []byte("meta")
	instructionBucket = []byte("instructions")
	installationIDKey = []byte("installation_id")
)

type Status string

const (
	StatusReserved Status = "reserved"
	StatusActive   Status = "active"
	StatusTerminal Status = "terminal"
	StatusUnknown  Status = "unknown"
)

type Record struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Status    Status    `json:"status"`
	Result    string    `json:"result,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Store struct {
	db *bolt.DB
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	store := &Store{db: db}
	if err := store.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initialize() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return fmt.Errorf("create meta bucket: %w", err)
		}
		instructions, err := tx.CreateBucketIfNotExists(instructionBucket)
		if err != nil {
			return fmt.Errorf("create instruction bucket: %w", err)
		}
		if meta.Get(installationIDKey) == nil {
			id, err := uuid.NewRandom()
			if err != nil {
				return fmt.Errorf("generate installation id: %w", err)
			}
			if err := meta.Put(installationIDKey, []byte(id.String())); err != nil {
				return fmt.Errorf("store installation id: %w", err)
			}
		}

		now := time.Now().UTC()
		return instructions.ForEach(func(key, value []byte) error {
			var record Record
			if err := json.Unmarshal(value, &record); err != nil {
				return fmt.Errorf("decode instruction %q: %w", key, err)
			}
			if record.Status != StatusReserved && record.Status != StatusActive {
				return nil
			}
			record.Status = StatusUnknown
			record.Result = "agent process restarted before a terminal event"
			record.UpdatedAt = now
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			return instructions.Put(key, encoded)
		})
	})
}

func (s *Store) InstallationID() (string, error) {
	var id string
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(metaBucket).Get(installationIDKey)
		if value == nil {
			return errors.New("installation id is absent")
		}
		id = string(value)
		return nil
	})
	return id, err
}

func (s *Store) Reserve(id, kind string) (bool, error) {
	if err := validateID(id); err != nil {
		return false, err
	}
	if kind == "" {
		return false, errors.New("instruction kind is empty")
	}
	reserved := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(instructionBucket)
		if bucket.Get([]byte(id)) != nil {
			return nil
		}
		now := time.Now().UTC()
		record := Record{ID: id, Kind: kind, Status: StatusReserved, CreatedAt: now, UpdatedAt: now}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(id), encoded); err != nil {
			return err
		}
		reserved = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("reserve instruction: %w", err)
	}
	return reserved, nil
}

func (s *Store) Activate(id string) error {
	return s.update(id, func(record *Record) error {
		if record.Status != StatusReserved {
			return fmt.Errorf("instruction %q is %s, expected reserved", id, record.Status)
		}
		record.Status = StatusActive
		return nil
	})
}

func (s *Store) Complete(id, result string) error {
	return s.update(id, func(record *Record) error {
		if record.Status == StatusTerminal || record.Status == StatusUnknown {
			return nil
		}
		record.Status = StatusTerminal
		record.Result = result
		return nil
	})
}

func (s *Store) Record(id string) (Record, bool, error) {
	if err := validateID(id); err != nil {
		return Record{}, false, err
	}
	var record Record
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(instructionBucket).Get([]byte(id))
		if value == nil {
			return nil
		}
		found = true
		return json.Unmarshal(value, &record)
	})
	return record, found, err
}

func (s *Store) update(id string, mutate func(*Record) error) error {
	if err := validateID(id); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(instructionBucket)
		value := bucket.Get([]byte(id))
		if value == nil {
			return fmt.Errorf("instruction %q is not reserved", id)
		}
		var record Record
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("decode instruction %q: %w", id, err)
		}
		if err := mutate(&record); err != nil {
			return err
		}
		record.UpdatedAt = time.Now().UTC()
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(id), encoded)
	})
}

func validateID(id string) error {
	if id == "" {
		return errors.New("instruction id is empty")
	}
	if len(id) > 4096 {
		return errors.New("instruction id exceeds 4096 bytes")
	}
	return nil
}
