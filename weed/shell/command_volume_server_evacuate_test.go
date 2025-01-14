package shell

import (
	"os"
	"testing"
)

func TestVolumeServerEvacuate(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	volumeServer := "192.168.1.4:8080"

	c := commandVolumeServerEvacuate{}
	if err := c.evacuateNormalVolumes(nil, topologyInfo, volumeServer, true, false, os.Stdout); err != nil {
		t.Errorf("evacuate: %v", err)
	}

}
