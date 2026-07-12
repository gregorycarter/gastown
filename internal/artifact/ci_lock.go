package artifact

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

const ciMaintenanceProtocolScript = "ci-maintenance-protocol.sh"

type ciProtocolInvoker func(containerID, helperPath, action, nonce string) ([]byte, error)

// AcquireCIMaintenanceLock asks a previously verified runner container to
// acquire the shared mkdir-v1 maintenance protocol. The helper executes in the
// same Linux filesystem view as job hooks, so atomic mkdir—not host filesystem
// behavior—is the serialization primitive.
func AcquireCIMaintenanceLock(root, containerID string) (func() error, error) {
	return acquireCIMaintenanceLock(root, containerID, invokeCIProtocol)
}

func acquireCIMaintenanceLock(root, containerID string, invoke ciProtocolInvoker) (func() error, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) == string(filepath.Separator) {
		return nil, fmt.Errorf("CI maintenance root must be an absolute non-root path")
	}
	if strings.TrimSpace(containerID) == "" {
		return nil, fmt.Errorf("verified CI runner container is required")
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, fmt.Errorf("creating CI maintenance nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)
	helperPath := filepath.Join(root, "scripts", ciMaintenanceProtocolScript)
	out, err := invoke(containerID, helperPath, "acquire", nonce)
	if err != nil {
		return nil, fmt.Errorf("acquiring CI maintenance protocol: %w", err)
	}
	if attestErr := validateCIProtocolAttestation(out, "acquired", nonce); attestErr != nil {
		// A zero exit status means the helper may have published maintenance
		// intent. Release only our nonce before returning the attestation error.
		releaseOut, releaseErr := invoke(containerID, helperPath, "release", nonce)
		if releaseErr == nil {
			releaseErr = validateCIProtocolAttestation(releaseOut, "released", nonce)
		}
		if releaseErr != nil {
			releaseErr = fmt.Errorf("releasing CI maintenance after invalid acquire attestation: %w", releaseErr)
		}
		return nil, errors.Join(attestErr, releaseErr)
	}

	return func() error {
		out, err := invoke(containerID, helperPath, "release", nonce)
		if err != nil {
			return fmt.Errorf("releasing CI maintenance protocol: %w", err)
		}
		if err := validateCIProtocolAttestation(out, "released", nonce); err != nil {
			return fmt.Errorf("releasing CI maintenance protocol: %w", err)
		}
		return nil
	}, nil
}

func invokeCIProtocol(containerID, helperPath, action, nonce string) ([]byte, error) {
	cmd := exec.Command("docker", "exec", containerID, helperPath, action, nonce) //nolint:gosec // fixed executable; id and helper were verified before acquisition
	out, err := cmd.Output()
	if err == nil {
		return out, nil
	}
	detail := ""
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		detail = strings.TrimSpace(string(exitErr.Stderr))
	}
	if detail == "" {
		return out, err
	}
	return out, fmt.Errorf("%w: %s", err, detail)
}

func validateCIProtocolAttestation(out []byte, action, nonce string) error {
	expected := action + ":" + nonce + "\n"
	if string(out) != expected {
		return fmt.Errorf("CI maintenance helper returned invalid %s attestation", action)
	}
	return nil
}
