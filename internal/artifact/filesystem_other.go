//go:build !unix

package artifact

import "io/fs"

// Symlink/reparse-point checks and os.Root confinement still apply on non-Unix
// platforms. A portable filesystem identity is not exposed by os.FileInfo.
func filesystemID(_ fs.FileInfo) (uint64, bool) {
	return 0, false
}
