//go:build linux

package artifact

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var linuxMounts struct {
	sync.Once
	paths map[string]bool
	err   error
}

func isMountPoint(candidate string) (bool, error) {
	linuxMounts.Do(func() {
		data, err := os.ReadFile("/proc/self/mountinfo")
		if err != nil {
			linuxMounts.err = err
			return
		}
		linuxMounts.paths = make(map[string]bool)
		unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				continue
			}
			linuxMounts.paths[filepath.Clean(unescape.Replace(fields[4]))] = true
		}
	})
	return linuxMounts.paths[filepath.Clean(candidate)], linuxMounts.err
}
