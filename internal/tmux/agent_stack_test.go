package tmux

import (
	"reflect"
	"testing"
)

// kasb-2ifwk: an idle agent's own processes sit in its worktree; the work it
// starts (make -> uv -> pytest) must stay visible as sandbox activity.
func TestAgentStackFromSnapshot(t *testing.T) {
	codexBin := "/opt/homebrew/lib/node_modules/@openai/codex/vendor/aarch64-apple-darwin/bin/codex"
	for _, tc := range []struct {
		name  string
		root  string
		names []string
		ps    string
		want  []int
	}{
		{
			name:  "codex pane with helper and a test run",
			root:  "83037",
			names: []string{"codex"},
			ps: "83037 1 " + codexBin + "\n" +
				"84197 83037 " + codexBin + "-code-mode-host\n" +
				"34958 83037 /usr/bin/make\n" +
				"35037 34958 /bin/sh\n" +
				"35041 35037 /town/kasb_core/polecats/nuka/kasb_core/.venv/bin/python3\n",
			want: []int{83037, 84197},
		},
		{
			name:  "login shell pane running claude with a shell tool call",
			root:  "100",
			names: []string{"node", "claude"},
			ps: "100 1 -zsh\n" +
				"200 100 /Users/x/.local/bin/claude\n" +
				"300 200 /bin/zsh\n" +
				"301 300 node\n",
			want: []int{100, 200},
		},
		{
			name:  "agent binary not found: only the pane and shells",
			root:  "100",
			names: []string{"codex"},
			ps: "100 1 zsh\n" +
				"200 100 node\n" +
				"201 200 codex\n",
			want: []int{100},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := agentStackFromSnapshot(tc.root, tc.names, parseProcessSnapshot([]byte(tc.ps)))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("agentStackFromSnapshot = %v, want %v", got, tc.want)
			}
		})
	}
}
