// Package buildrev identifies the code inside a Relay image, so recorded
// results name the commit they measured.
//
// The Makefile passes `git rev-parse HEAD` (suffixed "-dirty" when the
// working tree has uncommitted changes) to the image build, which stores it
// as the OCI revision label.
package buildrev

import (
	"os/exec"
	"strings"
)

// Label is the image label holding the git revision.
const Label = "org.opencontainers.image.revision"

// Image describes one local image.
type Image struct {
	Ref      string `json:"ref"`
	ID       string `json:"id"`
	Revision string `json:"revision"`
	Created  string `json:"created"`
}

// Inspect returns what Docker knows about ref. Revision is "unknown" for an
// image built without the label (for example by `docker build` directly).
func Inspect(ref string) Image {
	img := Image{Ref: ref, Revision: "unknown"}
	out, err := exec.Command("docker", "image", "inspect", "--format",
		`{{.Id}}|{{.Created}}|{{index .Config.Labels "`+Label+`"}}`, ref).Output()
	if err != nil {
		return img
	}
	parts := strings.SplitN(strings.TrimSpace(string(out)), "|", 3)
	if len(parts) == 3 {
		img.ID, img.Created = parts[0], parts[1]
		if rev := parts[2]; rev != "" && rev != "<no value>" && rev != "unknown" {
			img.Revision = rev
		}
	}
	return img
}
