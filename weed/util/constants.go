package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 2.82)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
	SPECIAL        = "Image Cache"
)

func Version() string {
	return VERSION + " " + COMMIT + " " + SPECIAL
}
