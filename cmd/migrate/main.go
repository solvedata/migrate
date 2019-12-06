package main

import (
	_ "github.com/solvedata/migrate/database/ksql"
	"github.com/solvedata/migrate/v4/internal/cli"
)

func main() {
	cli.Main(Version)
}
