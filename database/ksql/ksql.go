package ksql

import (
	"bytes"
	"encoding/json"
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

var CreateMigrationStreamSQL = `CREATE STREAM migrations
  (type VARCHAR,
  current_version INT,
  is_dirty BOOLEAN)
  WITH (KAFKA_TOPIC = 'schema_migrations',
        VALUE_FORMAT='JSON',
        KEY = 'type',
        PARTITIONS = 1);`
var CreateMigrationTableSQL = `CREATE TABLE schema_migrations AS
  SELECT MAX(ROWTIME), type FROM migrations GROUP BY type;`
var LatestSchemaRowTimeSQL = `SELECT * FROM schema_migrations WHERE type = 'schema' LIMIT 1;`

// var LatestSchemaMigrationSQL = `SELECT * FROM schema_migrations
//   WHERE type = 'schema' LIMIT 1;`

// INSERT INTO migrations VALUES ('schema', 'schema', 1, false);
// INSERT INTO migrations VALUES ('schema', 'schema', 3, true);
// INSERT INTO migrations VALUES ('schema', 'schema', , false);
// create table test as select max(current_version), type from migrations group by type;
// select * from test where type = 'schema' limit 1;
// select * from schema_migrations where rowtime = 1576119064224 limit 1;

type SavedMigrationResult struct {
	Row SavedMigrationRow
}

type SavedMigrationRow struct {
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
	// Create HTTP client to use
	client := &http.Client{}
	httpUrl := strings.Replace(url, "ksql://", "http://", 1)

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
	resp, err := s.runKsql(query)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		printResponseBody(resp)
		return errors.New(fmt.Sprintf("Unexpected response code of %v", resp.Status))
	}

	return nil
}

func (s *Ksql) SetVersion(version int, dirty bool) error {
	if version >= 0 {
		query := fmt.Sprintf("INSERT INTO migrations VALUES ('schema', 'schema', %v, %v);", version, dirty)
		resp, err := s.runKsql(query)
		if err != nil {
			return nil
		}
		printResponseBody(resp)
		s.CurrentVersion = version
		s.IsDirty = dirty
	}
	return nil
}

func (s *Ksql) Version() (version int, dirty bool, err error) {
	fmt.Println("Version:", version)
	if s.FirstRun {
		fmt.Println("First run, no version to set")
		return -1, false, nil
	}

	resp, err := s.runQuery(LatestSchemaRowTimeSQL)
	if err != nil {
		fmt.Println("Error getting current version, not setting")
		return -1, false, err
	}

	body := strings.Trim(resposeBodyText(resp), "\n")
	lines := strings.Split(body, "\n")

	var latestTimeRows SavedMigrationResult
	err = json.Unmarshal([]byte(lines[0]), &latestTimeRows)
	if err != nil {
		return -1, false, err
	}

	fmt.Println("latest", latestTimeRows)

	rowtime := latestTimeRows.Row.Columns[0]
	latestSchemaMigrationSql := fmt.Sprintf(`SELECT * FROM migrations WHERE rowtime = %v LIMIT 1;`, rowtime)

	resp, err = s.runQuery(latestSchemaMigrationSql)
	if err != nil {
		fmt.Println("Error getting current version, not setting")
		return -1, false, err
	}
	body = strings.Trim(resposeBodyText(resp), "\n")
	lines = strings.Split(body, "\n")

	var latestMigrationRows SavedMigrationResult
	err = json.Unmarshal([]byte(lines[0]), &latestMigrationRows)
	if err != nil {
		return -1, false, err
	}

	i := int(latestMigrationRows.Row.Columns[3].(float64))
	fmt.Println(i)
	// i, err = strconv.ParseInt(latestMigrationRows.Row.Columns[3].(string), 10, 32)
	if err != nil {
		return -1, false, err
	}
	fmt.Println("latest", i)

	return i, latestMigrationRows.Row.Columns[4].(bool), nil
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
		return false
	}

	return resp.Status != "200"
}

func (s *Ksql) ensureVersionTable() (err error) {
	stmt := "LIST TABLES;"
	resp, err := s.runKsql(stmt)
	if err != nil {
		return err
	}

	lowerCaseBody := strings.ToLower(resposeBodyText(resp))
	tableExists := strings.Contains(lowerCaseBody, "schema_migrations")

	if tableExists {
		fmt.Println("Exists")
		s.FirstRun = false
		return nil
	}

	fmt.Println("Table does not exist. Creating stream")

	resp, err = s.runKsql(CreateMigrationStreamSQL)
	if err != nil {
		return err
	}

	fmt.Println("Table does not exist. Creating table")

	resp, err = s.runKsql(CreateMigrationTableSQL)
	if err != nil {
		return err
	}

	fmt.Println("Creation done!")

	return nil
}

func (s *Ksql) runKsql(query string) (*http.Response, error) {
	url := fmt.Sprintf(`%v/ksql`, s.HttpUrl)
	return s.doQuery(url, query)
}

func (s *Ksql) runQuery(query string) (*http.Response, error) {
	url := fmt.Sprintf(`%v/query`, s.HttpUrl)
	fmt.Println(url, query)
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

func resposeBodyText(resp *http.Response) string {
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error printing response body", err)
		return ""
	}
	return string(bodyBytes)
}

func printResponseBody(resp *http.Response) {
	bodyString := resposeBodyText(resp)
	fmt.Println(bodyString)
}
