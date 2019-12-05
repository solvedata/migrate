package ksql

import (
	"io"
	"io/ioutil"
	"reflect"

	"github.com/solvedata/migrate/v4/database"
)

func init() {
	database.Register("ksql", &Ksql{})
}

type Ksql struct {
	Url               string
	Instance          interface{}
	CurrentVersion    int
	MigrationSequence []string
	LastRunMigration  []byte // todo: make []string
	IsDirty           bool
	IsLocked          bool

	Config *Config
}

func (s *Ksql) Open(url string) (database.Driver, error) {
	return &Ksql{
		Url:               url,
		CurrentVersion:    -1,
		MigrationSequence: make([]string, 0),
		Config:            &Config{},
	}, nil
}

type Config struct{}

func WithInstance(instance interface{}, config *Config) (database.Driver, error) {
	return &Ksql{
		Instance:          instance,
		CurrentVersion:    -1,
		MigrationSequence: make([]string, 0),
		Config:            config,
	}, nil
}

func (s *Ksql) Close() error {
	return nil
}

func (s *Ksql) Lock() error {
	if s.IsLocked {
		return database.ErrLocked
	}
	s.IsLocked = true
	return nil
}

func (s *Ksql) Unlock() error {
	s.IsLocked = false
	return nil
}

func (s *Ksql) Run(migration io.Reader) error {
	m, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	s.LastRunMigration = m
	s.MigrationSequence = append(s.MigrationSequence, string(m[:]))
	return nil
}

func (s *Ksql) SetVersion(version int, state bool) error {
	s.CurrentVersion = version
	s.IsDirty = state
	return nil
}

func (s *Ksql) Version() (version int, dirty bool, err error) {
	return s.CurrentVersion, s.IsDirty, nil
}

const DROP = "DROP"

func (s *Ksql) Drop() error {
	s.CurrentVersion = -1
	s.LastRunMigration = nil
	s.MigrationSequence = append(s.MigrationSequence, DROP)
	return nil
}

func (s *Ksql) EqualSequence(seq []string) bool {
	return reflect.DeepEqual(seq, s.MigrationSequence)
}
