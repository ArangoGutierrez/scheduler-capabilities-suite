package verdict_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// listingLine matches an instruction line of a -gcflags=-S listing:
// "\t0x0c28 03112 (/path/fairshare.go:224)\tFMADDD\tF1, F2, F0, F3". The
// path may contain spaces, so it is matched up to ".go:<line>)".
var listingLine = regexp.MustCompile(`\((.+\.go):(\d+)\)\t(\S+)`)

// listingNotParsed is the failure when no instruction line of the listing is
// attributed to fairshare.go: the listing was not read, so finding no fused
// instruction would prove nothing.
const listingNotParsed = "the -gcflags=-S listing could not be parsed: no instruction line is attributed to fairshare.go"

// fusedSites returns "file.go:line INSTR" for every instruction in listing
// whose mnemonic matches fused, and how many instruction lines came from
// fairshare.go.
func fusedSites(listing string, fused *regexp.Regexp) ([]string, int) {
	var out []string
	fromFairshare := 0
	for _, m := range listingLine.FindAllStringSubmatch(listing, -1) {
		if filepath.Base(m[1]) == "fairshare.go" {
			fromFairshare++
		}
		if fused.MatchString(m[3]) {
			out = append(out, filepath.Base(m[1])+":"+m[2]+" "+m[3])
		}
	}
	return out, fromFairshare
}

// Go may fuse x*y+z into one rounding (arm64 always may; amd64 may with
// GOAMD64=v3), and a fused result can differ in the last bit from the
// separately rounded one, so a run and its rebuild on another architecture
// would disagree. Every product that reaches a sum is rounded by an explicit
// float64(...); this checks the compiled code, so a new unwrapped product
// fails here even when no fixture's arithmetic shows the difference.
var _ = Describe("compiled verdict code", func() {
	// Label toolchain: two compiles take seconds, so make test filters these
	// out to stay within its 30 s budget and make lint runs them.
	DescribeTable("has no fused multiply-add instruction", Label("toolchain"),
		func(env []string, fused string) {
			goBin, err := exec.LookPath("go")
			Expect(err).NotTo(HaveOccurred(), "the go command must be on PATH")
			// a library package with -o /dev/null writes nothing; the
			// listing is replayed from the build cache when it is warm
			cmd := exec.Command(goBin, "build", "-o", os.DevNull, "-gcflags=-S", ".")
			cmd.Env = append(os.Environ(), env...)
			out, err := cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))
			listing := string(out)
			// an empty or partial listing must not pass
			Expect(listing).To(ContainSubstring("pkg/verdict.FairShare STEXT"))
			Expect(listing).To(ContainSubstring("pkg/verdict.Preemption STEXT"))
			sites, fromFairshare := fusedSites(listing, regexp.MustCompile(fused))
			if fromFairshare == 0 {
				Fail(listingNotParsed)
			}
			Expect(sites).To(BeEmpty())
		},
		Entry("arm64", []string{"GOARCH=arm64"}, `^FN?M(ADD|SUB)[DS]$`),
		Entry("amd64 with GOAMD64=v3", []string{"GOARCH=amd64", "GOAMD64=v3"}, `^VFN?M(ADD|SUB)`),
	)
})
