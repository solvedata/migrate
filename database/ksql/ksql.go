package ksql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/solvedata/migrate/v4/database"
)

func init() {
	database.Register("ksql", &Ksql{})
}

var CreateMigrationStreamSQL = `CREATE STREAM migrations
  (type VARCHAR,
  current_version INT,
  is_dirty BOOLEAN)
  WITH (KAFKA_TOPIC = 'ksql_migrations',
        VALUE_FORMAT='JSON',
        KEY = 'type',
        PARTITIONS = 1);`
var CreateMigrationTableSQL = `CREATE TABLE schema_migrations
  WITH (KAFKA_TOPIC = 'ksql_schema_migrations',
        VALUE_FORMAT='JSON',
        PARTITIONS = 1)
  AS SELECT MAX(current_version) as current_version, type FROM migrations
  WHERE NOT is_dirty
  GROUP BY type;`
var LatestSchemaMigrationSql = `SELECT current_version FROM schema_migrations WHERE type = 'schema' LIMIT 1;`

type MigrationResult struct {
	Row MigrationRow
}

type MigrationRow struct {
	Columns []interface{}
}

type Ksql struct {
	Url               string
	HttpUrl           string
	Instance          interface{}
	CurrentVersion    int
	MigrationSequence []string
	LastRunMigration  []byte // todo: make []string
	IsDirty           bool
	FirstRun          bool
	Client            *http.Client

	Config *Config
}

func (s *Ksql) Open(url string) (database.Driver, error) {
	fmt.Println("Opening at KSQL URL", url)
	// Create HTTP client to use
	timeout, err := strconv.ParseInt(os.Getenv("MIGRATE_KSQL_TIMEOUT"), 10, 64)
	if err != nil {
		fmt.Println("Unable to parse `MIGRATE_KSQL_TIMEOUT` environment variable. Defaulting to 10 seconds.")
		timeout = 10
	}
	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}
	httpUrl := strings.Replace(url, "ksql://", "http://", 1)
	fmt.Println("Setting HTTP URL with", httpUrl)

	// We have a URL - can we connect?

	ks := &Ksql{
		Url:               url,
		HttpUrl:           httpUrl,
		Client:            client,
		CurrentVersion:    -1,
		FirstRun:          true,
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
	return nil
}

func (s *Ksql) Unlock() error {
	return nil
}

func (s *Ksql) Run(migration io.Reader) error {
	m, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	s.LastRunMigration = m
	s.MigrationSequence = append(s.MigrationSequence, string(m[:]))

	query := string(m[:])
	// The migration is expecte to be valid KSQL. Send this to the KSQL server
	resp, err := s.runKsql(query)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		// Something unexpected happened. Print out the response body and error out.
		printResponseBody(resp)
		return errors.New(fmt.Sprintf("Unexpected response code of %v", resp.Status))
	}

	return nil
}

// Adds a new record with the current migration version and it's dirty state
func (s *Ksql) SetVersion(version int, dirty bool) error {
	if version >= 0 {
		query := fmt.Sprintf("INSERT INTO migrations VALUES ('schema', 'schema', %v, %v);", version, dirty)
		_, err := s.runKsql(query)
		if err != nil {
			return nil
		}

		// Version updated in migration table successfully. Update instance
		s.CurrentVersion = version
		s.IsDirty = dirty
	}
	return nil
}

// Retrieves the current version of the KSQL migration state
func (s *Ksql) Version() (version int, dirty bool, err error) {
	if s.FirstRun {
		// This is the first time _any_ migration has been run. No version to retrieve
		// See .ensureVersionTable for where this is set
		fmt.Println("First run, no version to set")
		return -1, false, nil
	}

	currentVersion, isDirty, err := s.getLatestMigration()
	if err != nil {
		fmt.Println("Error getting latest migration version")
		return -1, false, nil
	}

	return currentVersion, isDirty, nil
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
	resp, err := s.runKsql(query)
	if err != nil {
		fmt.Println("KSQL URL is not accepting requests")
		return false
	}

	return resp.Status != "200"
}

// Makes sure that the schema migration state table is setup correctly
func (s *Ksql) ensureVersionTable() (err error) {
	stmt := "LIST TABLES;"
	resp, err := s.runKsql(stmt)
	if err != nil {
		return err
	}

	body := resposeBodyText(resp)
	lowerCaseBody := strings.ToLower(body)
	// Simple check - does any text (i.e. table names) contain schema_migrations?
	tableExists := strings.Contains(lowerCaseBody, "schema_migrations")

	if tableExists {
		fmt.Println("Schema migrations table already exists")
		s.FirstRun = false
		return nil
	}

	fmt.Println("Schema migrations table does not exist. Creating stream")
	// First create the stream for the table to come off
	resp, err = s.runKsql(CreateMigrationStreamSQL)
	if err != nil {
		return err
	}

	fmt.Println("Schema migrations table does not exist. Creating table")
	// Now create the table itself
	resp, err = s.runKsql(CreateMigrationTableSQL)
	if err != nil {
		return err
	}

	fmt.Println("Schema migrations table creation done!")
	return nil
}

func (s *Ksql) runKsql(query string) (*http.Response, error) {
	url := fmt.Sprintf(`%v/ksql`, s.HttpUrl)
	return s.doQuery(url, query)
}

func (s *Ksql) runQuery(query string) (*http.Response, error) {
	url := fmt.Sprintf(`%v/query`, s.HttpUrl)
	return s.doQuery(url, query)
}

func (s *Ksql) doQuery(url string, query string) (*http.Response, error) {
	formatted_query := fmt.Sprintf(`{"ksql":"%v","streamsProperties":{ "ksql.streams.auto.offset.reset": "earliest"}}`, strings.Replace(query, "\n", " ", -1))
	req_body := []byte(formatted_query)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(req_body))

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

// Does a request for the most recent event in the migration table
func (s *Ksql) getLatestMigration() (int, bool, error) {
	resp, err := s.runQuery(fmt.Sprintf(LatestSchemaMigrationSql))
	if err != nil {
		return -1, false, err
	}
	result, err := responseBodyMigrationResult(resp)
	if err != nil {
		return -1, false, err
	}
	currentVersion := int(result.Row.Columns[0].(float64))
	fmt.Println("Current version:", currentVersion)

	return currentVersion, false, nil
}

// Helper to grab the first line in a response body (while also removing whitespace etc)
func responseBodyMigrationResult(resp *http.Response) (MigrationResult, error) {
	body := strings.Trim(resposeBodyText(resp), "\n")
	lines := strings.Split(body, "\n")

	var result MigrationResult
	err := json.Unmarshal([]byte(lines[0]), &result)
	if err != nil {
		return MigrationResult{}, err
	}

	return result, nil
}

// Helper to extract the HTTP response body
func resposeBodyText(resp *http.Response) string {
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(bodyBytes)
}

// Debuggering helper to print the HTTP response body
func printResponseBody(resp *http.Response) {
	bodyString := resposeBodyText(resp)
	fmt.Println(bodyString)
}
