//go:build unix

package artifact

import (
	"io/fs"
	"syscall"
)

func filesystemID(info fs.FileInfo) (uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(stat.Dev), true //nolint:unconvert // Dev needs conversion on Darwin but is already uint64 on Linux.
}
