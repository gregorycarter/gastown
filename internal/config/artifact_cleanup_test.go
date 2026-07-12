package config

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/artifact"
)

func cleanupBool(v bool) *bool    { return &v }
func cleanupInt64(v int64) *int64 { return &v }

func TestResolveArtifactCleanupPolicyMergesTownAndRigSettings(t *testing.T) {
	townRoot := t.TempDir()
	rigPath := filepath.Join(townRoot, "testrig")
	town := NewTownSettings()
	town.Lifecycle = &LifecycleConfig{Cleanup: &artifact.PolicyConfig{
		Enabled:             cleanupBool(true),
		Mode:                artifact.ModeApply,
		Paths:               []string{"target", "dist"},
		MaxAge:              "24h",
		MaxBytes:            cleanupInt64(1000),
		OnPolecatReuse:      cleanupBool(true),
		AllowProtectedPaths: []string{"data/raw"}, // town override must be ignored
	}}
	if err := SaveTownSettings(TownSettingsPath(townRoot), town); err != nil {
		t.Fatal(err)
	}
	rig := NewRigSettings()
	rig.Lifecycle = &LifecycleConfig{Cleanup: &artifact.PolicyConfig{
		Paths:               []string{"dist", "data/raw"},
		MaxAge:              "48h",
		AllowProtectedPaths: []string{"data/raw"},
	}}
	if err := SaveRigSettings(RigSettingsPath(rigPath), rig); err != nil {
		t.Fatal(err)
	}

	policy, err := ResolveArtifactCleanupPolicy(townRoot, rigPath)
	if err != nil {
		t.Fatal(err)
	}
	if !policy.Enabled || policy.Mode != artifact.ModeApply || policy.MaxAge != 48*time.Hour || policy.MaxBytes != 1000 || !policy.OnPolecatReuse {
		t.Fatalf("unexpected merged policy: %+v", policy)
	}
	if len(policy.AllowProtectedPaths) != 1 || policy.AllowProtectedPaths[0] != "data/raw" {
		t.Fatalf("rig protected override missing: %+v", policy.AllowProtectedPaths)
	}
}

func TestRigSettingsRejectPermanentProtectedOverride(t *testing.T) {
	settings := NewRigSettings()
	settings.Lifecycle = &LifecycleConfig{Cleanup: &artifact.PolicyConfig{
		AllowProtectedPaths: []string{".dolt-data"},
	}}
	if err := SaveRigSettings(filepath.Join(t.TempDir(), "settings", "config.json"), settings); err == nil {
		t.Fatal("expected permanent protected override to be rejected")
	}
}
