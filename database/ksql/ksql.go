package ksql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/solvedata/migrate/v4/database"
)

func init() {
	database.Register("ksql", &Ksql{})
}

var DefaultMigrationsTableAndTopic = "schema_migraions"
var CreateTableSQL = `CREATE TABLE schema_migrations
  (registertime BIGINT,
  id VARCHAR,
  blob VARCHAR,
  created_at VARCHAR)
  WITH (KAFKA_TOPIC = 'schema_migrations',
        VALUE_FORMAT='JSON',
        KEY = 'id',
        PARTITIONS = 1);`

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
	// Create HTTP client to use
	client := &http.Client{}
	httpUrl := strings.Replace(url, "ksql://", "http://", 1)

	// We have a URL - can we connect?

	ks := &Ksql{
		Url:               url,
		HttpUrl:           httpUrl,
		Client:            client,
		CurrentVersion:    -1,
		MigrationSequence: make([]string, 0),
		Config:            &Config{},
	}

	hasConnection := ks.ensureUrlConection()

	if !hasConnection {
		return nil, errors.New(fmt.Sprintf("Cannot connect to KSQL at %v", s.HttpUrl))
	}

	if err := ks.ensureVersionTable(); err != nil {
		return nil, err
	}

	return ks, nil
}

type Config struct{}

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

	fmt.Print(s.MigrationSequence)

	query := string(m[:])
	resp, err := s.runQuery(query)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		printResponseBody(resp)
		return errors.New(fmt.Sprintf("Unexpected response code of %v", resp.Status))
	}

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

func (s *Ksql) Drop() error {
	s.CurrentVersion = -1
	s.LastRunMigration = nil
	s.MigrationSequence = append(s.MigrationSequence, "DROP")
	return nil
}

func (s *Ksql) ensureUrlConection() bool {
	// Check that we can run a query with the given URL
	query := "LIST TOPICS;"
	resp, err := s.runQuery(query)
	if err != nil {
		return false
	}

	return resp.Status != "200"
}

func (s *Ksql) ensureVersionTable() (err error) {
	stmt := "LIST TABLES;"
	resp, err := s.runQuery(stmt)
	if err != nil {
		return err
	}

	tableExists := strings.Contains(reposeBodyText(resp), DefaultMigrationsTableAndTopic)

	if tableExists {
		fmt.Println("Table exists")
		return nil
	}

	resp, err = s.runQuery(CreateTableSQL)
	if err != nil {
		return err
	}

	return nil
}

func (s *Ksql) runQuery(query string) (*http.Response, error) {
	formatted_query := fmt.Sprintf(`{"ksql":"%v","streamsProperties":{ "ksql.streams.auto.offset.reset": "earliest"}}`, strings.Replace(query, "\n", " ", -1))
	req_body := []byte(formatted_query)
	req, err := http.NewRequest("POST", s.HttpUrl, bytes.NewBuffer(req_body))

	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8")
	resp, err := s.Client.Do(req)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func reposeBodyText(resp *http.Response) string {
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error printing response body", err)
		return ""
	}
	return string(bodyBytes)
}

func printResponseBody(resp *http.Response) {
	bodyString := reposeBodyText(resp)
	fmt.Println(bodyString)
}
