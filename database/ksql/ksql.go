package ksql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/solvedata/migrate/v4/database"
)

func init() {
	database.Register("ksql", &Ksql{})
}

var DefaultMigrationsTopic = "migrations"

type Ksql struct {
	Url               string
	HttpUrl           string
	Instance          interface{}
	CurrentVersion    int
	MigrationSequence []string
	LastRunMigration  []byte // todo: make []string
	IsDirty           bool
	IsLocked          bool
	Client            *http.Client

	Config *Config
}

func (s *Ksql) Open(url string) (database.Driver, error) {
	fmt.Println("Open")
	// Create HTTP client to use
	client := &http.Client{}
	httpUrl := strings.Replace(url, "ksql://", "http://", 1)
	fmt.Println(httpUrl)

	// We have a URL - can we connect?
	return &Ksql{
		Url:               url,
		HttpUrl:           httpUrl,
		Client:            client,
		CurrentVersion:    -1,
		MigrationSequence: make([]string, 0),
		Config:            &Config{},
	}, nil
}

type Config struct{}

func WithInstance(instance interface{}, config *Config) (database.Driver, error) {
	fmt.Println("WithInstance")

	return &Ksql{
		Instance:          instance,
		CurrentVersion:    -1,
		MigrationSequence: make([]string, 0),
		Config:            config,
	}, nil
}

func (s *Ksql) Close() error {
	fmt.Println("Close")
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
	hasConnection := s.ensureUrlConection()

	if !hasConnection {
		return errors.New(fmt.Sprintf("Cannot connect to KSQL at %v", s.HttpUrl))
	}

	fmt.Println("Run")
	m, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	s.LastRunMigration = m
	s.MigrationSequence = append(s.MigrationSequence, string(m[:]))
	return nil
}

func (s *Ksql) SetVersion(version int, state bool) error {
	fmt.Println("Set version")
	s.CurrentVersion = version
	s.IsDirty = state
	return nil
}

func (s *Ksql) Version() (version int, dirty bool, err error) {
	return s.CurrentVersion, s.IsDirty, nil
}

const DROP = "DROP"

func (s *Ksql) Drop() error {
	fmt.Println("Drop")
	s.CurrentVersion = -1
	s.LastRunMigration = nil
	s.MigrationSequence = append(s.MigrationSequence, DROP)
	return nil
}

func (s *Ksql) EqualSequence(seq []string) bool {
	return reflect.DeepEqual(seq, s.MigrationSequence)
}

func (s *Ksql) ensureUrlConection() bool {
	fmt.Println("Ensure connection")
	// Check that we can run a query with the given URL
	query := "LIST TOPICS;"
	resp, err := s.runQuery(query)
	if err != nil {
		return false
	}

	return resp.Status != "200"
}

func (s *Ksql) runQuery(query string) (*http.Response, error) {
	req_body := []byte(fmt.Sprintf(`{\"ksql\":\"%v\",\"streamsProperties\":{}}`, query))
	req, err := http.NewRequest("POST", s.HttpUrl, bytes.NewBuffer(req_body))

	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", `[{"key":"Content-Type","name":"Content-Type","value":"application/vnd.ksql.v1+json; charset=utf-8","description":"","type":"text"}]`)
	resp, err := s.Client.Do(req)

	if err != nil {
		return nil, err
	}

	return resp, nil
}
