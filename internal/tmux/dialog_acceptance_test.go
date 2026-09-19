package tmux

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestAcceptWorkspaceTrustDialog_NoDialog verifies that when no trust dialog
// is present (agent prompt visible), the function returns quickly without error.
func TestAcceptWorkspaceTrustDialog_NoDialog(t *testing.T) {
	tm := newTestTmux(t)
	sessionName := "gt-test-trust-nodlg-" + t.Name()

	_ = tm.KillSession(sessionName)
	if err := tm.NewSession(sessionName, ""); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = tm.KillSession(sessionName) }()

	// Session starts with a shell prompt containing ">", "$", or "%"
	// The polling loop should exit early when it sees the prompt.
	start := time.Now()
	err := tm.AcceptWorkspaceTrustDialog(sessionName)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("AcceptWorkspaceTrustDialog: %v", err)
	}

	// Should complete well before the 8s timeout since prompt is visible
	if elapsed > 6*time.Second {
		t.Errorf("took %v, expected early exit (< 6s)", elapsed)
	}
}

// TestAcceptWorkspaceTrustDialog_DetectsDialog verifies that when trust dialog
// text appears in the pane, it is detected and accepted (Enter key sent).
func TestAcceptWorkspaceTrustDialog_DetectsDialog(t *testing.T) {
	tm := newTestTmux(t)
	sessionName := "gt-test-trust-dlg-" + t.Name()

	_ = tm.KillSession(sessionName)
	if err := tm.NewSession(sessionName, ""); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = tm.KillSession(sessionName) }()

	// Simulate the trust dialog by echoing its text into the pane
	if err := tm.SendKeys(sessionName, "echo 'Quick safety check - do you trust this folder?'"); err != nil {
		t.Fatalf("SendKeys: %v", err)
	}
	// Give the echo a moment to execute
	time.Sleep(300 * time.Millisecond)

	err := tm.AcceptWorkspaceTrustDialog(sessionName)
	if err != nil {
		t.Fatalf("AcceptWorkspaceTrustDialog: %v", err)
	}

	// Verify that Enter was sent (we can't easily verify the exact keypress,
	// but the function should return without error after detecting the dialog)
}

// TestAcceptWorkspaceTrustDialog_DetectsCodexDialog verifies that Codex's
// workspace trust prompt is treated as a trust dialog instead of an agent prompt.
func TestAcceptWorkspaceTrustDialog_DetectsCodexDialog(t *testing.T) {
	tm := newTestTmux(t)
	sessionName := "gt-test-trust-codex-" + t.Name()

	_ = tm.KillSession(sessionName)
	if err := tm.NewSession(sessionName, ""); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = tm.KillSession(sessionName) }()

	if err := tm.SendKeys(sessionName, "echo '> You are in /tmp/demo'; echo 'Do you trust the contents of this directory?'"); err != nil {
		t.Fatalf("SendKeys: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	if err := tm.AcceptWorkspaceTrustDialog(sessionName); err != nil {
		t.Fatalf("AcceptWorkspaceTrustDialog: %v", err)
	}
}

// TestAcceptBypassPermissionsWarning_NoDialog verifies that when no bypass
// permissions dialog is present, the function returns quickly without error.
func TestAcceptBypassPermissionsWarning_NoDialog(t *testing.T) {
	tm := newTestTmux(t)
	sessionName := "gt-test-bypass-nodlg-" + t.Name()

	_ = tm.KillSession(sessionName)
	if err := tm.NewSession(sessionName, ""); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = tm.KillSession(sessionName) }()

	start := time.Now()
	err := tm.AcceptBypassPermissionsWarning(sessionName)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("AcceptBypassPermissionsWarning: %v", err)
	}

	if elapsed > 6*time.Second {
		t.Errorf("took %v, expected early exit (< 6s)", elapsed)
	}
}

// TestAcceptBypassPermissionsWarning_DetectsDialog verifies that when bypass
// permissions dialog text appears in the pane, it is detected and accepted.
func TestAcceptBypassPermissionsWarning_DetectsDialog(t *testing.T) {
	tm := newTestTmux(t)
	sessionName := "gt-test-bypass-dlg-" + t.Name()

	_ = tm.KillSession(sessionName)
	if err := tm.NewSession(sessionName, ""); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = tm.KillSession(sessionName) }()

	// Simulate the bypass permissions dialog
	if err := tm.SendKeys(sessionName, "echo 'Bypass Permissions mode is enabled'"); err != nil {
		t.Fatalf("SendKeys: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	err := tm.AcceptBypassPermissionsWarning(sessionName)
	if err != nil {
		t.Fatalf("AcceptBypassPermissionsWarning: %v", err)
	}
}

// TestAcceptStartupDialogs_NoDialogs verifies the combined function returns
// quickly when no dialogs are present.
func TestAcceptStartupDialogs_NoDialogs(t *testing.T) {
	tm := newTestTmux(t)
	sessionName := "gt-test-startup-nodlg-" + t.Name()

	_ = tm.KillSession(sessionName)
	if err := tm.NewSession(sessionName, ""); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = tm.KillSession(sessionName) }()

	start := time.Now()
	err := tm.AcceptStartupDialogs(sessionName)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("AcceptStartupDialogs: %v", err)
	}

	// Both dialog checks should early-exit when prompt is visible
	if elapsed > 12*time.Second {
		t.Errorf("took %v, expected faster completion", elapsed)
	}
}

// TestAcceptWorkspaceTrustDialog_InvalidSession verifies error handling
// when the session doesn't exist.
func TestAcceptWorkspaceTrustDialog_InvalidSession(t *testing.T) {
	tm := newTestTmux(t)

	// Should not panic or hang — should return nil after timeout
	err := tm.AcceptWorkspaceTrustDialog("gt-nonexistent-session-xyz")
	// CapturePane errors are retried until timeout, then returns nil
	if err != nil {
		t.Fatalf("expected nil error for nonexistent session, got: %v", err)
	}
}

// TestContainsPromptIndicator verifies the prompt detection helper
// recognizes various shell and agent prompt patterns.
func TestContainsPromptIndicator(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{"claude prompt", "Hello! How can I help?\n>", true},
		{"codex prompt", "Ready\n› ", true},
		{"bash prompt", "user@host:~$", true},
		{"zsh prompt", "╰─❯", true},
		{"root prompt", "root@host:~#", true},
		{"csh prompt", "host%", true},
		{"dialog text only", "Quick safety check\nDo you trust this folder?", false},
		{"empty", "", false},
		{"whitespace only", "   \n  \n  ", false},
		{"bypass dialog", "Bypass Permissions mode\n1. No\n2. Yes, I accept", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsPromptIndicator(tt.content)
			if got != tt.want {
				t.Errorf("containsPromptIndicator(%q) = %v, want %v", tt.content, got, tt.want)
			}
		})
	}
}

func TestContainsWorkspaceTrustDialog(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{"claude trust prompt", "Quick safety check\nDo you trust this folder?", true},
		{"codex trust prompt", "> You are in /tmp/demo\nDo you trust the contents of this directory?", true},
		{"bypass dialog", "Bypass Permissions mode\n1. No\n2. Yes, I accept", false},
		{"shell prompt", "user@host:~$", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsWorkspaceTrustDialog(tt.content)
			if got != tt.want {
				t.Errorf("containsWorkspaceTrustDialog(%q) = %v, want %v", tt.content, got, tt.want)
			}
		})
	}
}

func TestContainsBlockingStartupDialog(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		content     string
		wantBlocked bool
		wantName    string
	}{
		{
			name: "codex update modal",
			content: `Update available! 0.137.0 -> 0.138.0
Update now
Skip
Skip until next version`,
			wantBlocked: true,
			wantName:    "codex update prompt",
		},
		{
			name:        "codex trust modal",
			content:     "> You are in /tmp/demo\nDo you trust the contents of this directory?",
			wantBlocked: true,
			wantName:    "workspace trust prompt",
		},
		{
			name:        "bypass modal",
			content:     "Bypass Permissions mode\n1. No\n2. Yes, I accept",
			wantBlocked: true,
			wantName:    "bypass permissions prompt",
		},
		{
			name:        "ready prompt",
			content:     "› ",
			wantBlocked: false,
		},
		{
			name: "stale bypass dialog before codex prompt",
			content: `Bypass Permissions mode
1. No
2. Yes, I accept
› `,
			wantBlocked: false,
		},
		{
			name: "stale bypass dialog before prompt and status",
			content: `Bypass Permissions mode
1. No
2. Yes, I accept
›
session ready`,
			wantBlocked: false,
		},
		{
			name: "stale trust dialog before shell prompt",
			content: `Quick safety check
Do you trust this folder?
user@host:~$`,
			wantBlocked: false,
		},
		{
			name: "old shell prompt before current bypass dialog",
			content: `user@host:~$
Bypass Permissions mode
1. No
2. Yes, I accept`,
			wantBlocked: true,
			wantName:    "bypass permissions prompt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotBlocked := containsBlockingStartupDialog(tt.content)
			if gotBlocked != tt.wantBlocked {
				t.Fatalf("blocked = %v, want %v", gotBlocked, tt.wantBlocked)
			}
			if gotName != tt.wantName {
				t.Fatalf("name = %q, want %q", gotName, tt.wantName)
			}
		})
	}
}

func TestContainsBlockingStartupDialog_CodexComposer(t *testing.T) {
	t.Parallel()
	const trust = "Do you trust the contents of this directory?"
	const composer = "› Ask Codex to do anything"
	tests := []struct {
		name    string
		content string
		blocked bool
	}{
		{"stale trust with current composer", trust + "\n" + composer, false},
		{"stale trust with active response and composer", trust + "\n• Working\n\n" + composer + "\n  gpt-5.6-luna max · /tmp/demo", false},
		{"composer whitespace", trust + "\n  ›\u00a0Ask Codex to do anything  ", false},
		{"stale bypass with composer", "Bypass Permissions mode\n1. No\n2. Yes, I accept\n" + composer, false},
		{"stale update with composer", "Update available!\nUpdate now\nSkip until next version\n" + composer, false},
		{"current trust after old composer", composer + "\n" + trust, true},
		{"current bypass after old composer", composer + "\nBypass Permissions mode", true},
		{"current update after old composer", composer + "\nUpdate available!\nUpdate now\nSkip until next version", true},
		{"selected trust option is not composer", trust + "\n› 1. Yes, continue\n  2. No, exit", true},
		{"arbitrary chevron text is not composer", trust + "\n› Yes, I trust this folder", true},
		{"quoted composer is not composer", trust + "\nExample: " + composer, true},
		{"unknown composer remains conservative", trust + "\n› Another placeholder", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, blocked := containsBlockingStartupDialog(tt.content)
			if blocked != tt.blocked {
				t.Fatalf("blocked = %v, want %v for %q", blocked, tt.blocked, tt.content)
			}
		})
	}
}

// fakeStartupPane serves a synthetic capture and records key sends without ever
// connecting to a real tmux socket, agent, or Beads database.
func fakeStartupPane(t *testing.T, content string) (*Tmux, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("synthetic tmux executable uses a POSIX shell")
	}
	dir := t.TempDir()
	pane := filepath.Join(dir, "pane.txt")
	keys := filepath.Join(dir, "keys.txt")
	if err := os.WriteFile(pane, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tmux"), []byte(`#!/bin/sh
case " $* " in
  *" capture-pane "*) /bin/cat "$GT_TEST_STARTUP_PANE" ;;
  *" send-keys "*) printf '%s\n' "$*" >> "$GT_TEST_STARTUP_KEYS" ;;
  *) exit 90 ;;
esac
`), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GT_TEST_STARTUP_PANE", pane)
	t.Setenv("GT_TEST_STARTUP_KEYS", keys)
	t.Setenv("PATH", dir)
	return NewTmuxWithSocket("hermetic-startup-test"), keys
}

func TestStartupDialogs_CodexComposerNoKeys(t *testing.T) {
	for _, oldDialog := range []string{
		"Do you trust the contents of this directory?",
		"Bypass Permissions mode\n1. No\n2. Yes, I accept",
	} {
		t.Run(oldDialog, func(t *testing.T) {
			tm, keys := fakeStartupPane(t, oldDialog+"\n• Working\n› Ask Codex to do anything")
			if err := tm.AcceptStartupDialogs("synthetic-worker"); err != nil {
				t.Fatal(err)
			}
			if err := tm.CheckStartupBlocked("synthetic-worker"); err != nil {
				t.Fatalf("running composer was treated as blocked: %v", err)
			}
			if _, err := os.Stat(keys); !os.IsNotExist(err) {
				t.Fatalf("stale dialog caused a key send: %v", err)
			}
		})
	}
}

func TestStartupDialogs_CurrentTrustStillAcceptedAndBlocked(t *testing.T) {
	tm, keys := fakeStartupPane(t, "› Ask Codex to do anything\nDo you trust the contents of this directory?\n› 1. Yes, continue\n2. No, exit")
	if err := tm.AcceptWorkspaceTrustDialog("synthetic-worker"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(keys)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 || !strings.HasSuffix(lines[0], " Enter") {
		t.Fatalf("expected existing single Enter acceptance, got %q", data)
	}
	// The fixture intentionally does not dismiss the modal after Enter. It must
	// still refuse startup, even though an old composer is in scrollback.
	if err := tm.CheckStartupBlocked("synthetic-worker"); err == nil || !strings.Contains(err.Error(), "workspace trust prompt") {
		t.Fatalf("undismissed genuine trust dialog was not refused: %v", err)
	}
}

// TestDismissStartupDialogsBlind_SendsKeys verifies that the blind dismiss
// sends keys without error on a valid session (no screen-scraping).
func TestDismissStartupDialogsBlind_SendsKeys(t *testing.T) {
	tm := newTestTmux(t)
	sessionName := "gt-test-blind-dismiss-" + t.Name()

	_ = tm.KillSession(sessionName)
	if err := tm.NewSession(sessionName, ""); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = tm.KillSession(sessionName) }()

	// Should complete quickly — no polling, no CapturePane
	start := time.Now()
	err := tm.DismissStartupDialogsBlind(sessionName)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("DismissStartupDialogsBlind: %v", err)
	}

	// Should take ~700ms (500ms + 200ms sleeps) — not the 8s+ dialog poll timeout
	if elapsed > 3*time.Second {
		t.Errorf("took %v, expected ~700ms (no polling)", elapsed)
	}
}

// TestDismissStartupDialogsBlind_InvalidSession verifies error handling
// when the session doesn't exist.
func TestDismissStartupDialogsBlind_InvalidSession(t *testing.T) {
	tm := newTestTmux(t)

	err := tm.DismissStartupDialogsBlind("gt-nonexistent-session-blind-xyz")
	// Should return an error since the session doesn't exist
	if err == nil {
		t.Error("expected error for nonexistent session, got nil")
	}
}
