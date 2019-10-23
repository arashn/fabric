package helium

import (
	"github.com/spf13/viper"
)

// HeliumDef contains Helium configuration parameters
type HeliumDef struct {
	Server      string
	DevicePaths string
}

// GetHeliumDefinition populates the HeliumDef config object
func GetHeliumDefinition() *HeliumDef {
	Server := viper.GetString("ledger.state.heliumConfig.heliumServer")
	DevicePaths := viper.GetString("ledger.state.heliumConfig.devicePaths")

	return &HeliumDef{Server, DevicePaths}
}
