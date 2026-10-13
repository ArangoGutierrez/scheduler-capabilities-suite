// Command scs-report rebuilds a run's verdicts.json files and summary.md
// from the run's recorded events, probes and claims.
//
//	scs-report [-out DIR] RUN_DIR
//
// Without -out it rewrites the files inside RUN_DIR.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/report"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("scs-report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("out", "", "directory to write into (default: RUN_DIR)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: scs-report [-out DIR] RUN_DIR")
		return 2
	}
	runDir := fs.Arg(0)
	dst := *out
	if dst == "" {
		dst = runDir
	}
	if err := report.Write(runDir, dst); err != nil {
		fmt.Fprintln(stderr, "scs-report:", err)
		return 1
	}
	return 0
}
