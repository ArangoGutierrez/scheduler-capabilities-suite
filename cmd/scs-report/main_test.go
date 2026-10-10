package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestScsReport(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "scs-report") {
		t.Fatal("scs-report specs failed")
	}
}

var _ = Describe("run", func() {
	It("exits 2 with a usage line when RUN_DIR is missing", func() {
		var stderr bytes.Buffer
		Expect(run(nil, &stderr)).To(Equal(2))
		Expect(stderr.String()).To(Equal("usage: scs-report [-out DIR] RUN_DIR\n"))
	})

	It("exits 1 and names the problem when the run directory has no manifest", func() {
		dir := GinkgoT().TempDir()
		var stderr bytes.Buffer
		Expect(run([]string{dir}, &stderr)).To(Equal(1))
		Expect(stderr.String()).To(Equal("scs-report: open " + filepath.Join(dir, "manifest.json") + ": no such file or directory\n"))
	})

	It("with -out, fails on a run without events and writes nothing into the run directory", func() {
		run0 := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(run0, "manifest.json"), []byte(`{"run_id":"empty"}`), 0o644)).To(Succeed())
		var stderr bytes.Buffer
		Expect(run([]string{"-out", GinkgoT().TempDir(), run0}, &stderr)).To(Equal(1))
		Expect(stderr.String()).To(Equal("scs-report: " + run0 + ": no profile directory has an events.jsonl\n"))
		entries, err := os.ReadDir(run0)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1))
	})
})
