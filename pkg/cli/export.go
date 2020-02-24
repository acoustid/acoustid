package cli

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/acoustid/acoustid/pkg/export"
	"go.uber.org/zap"
	"errors"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export database to a remote location",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := zap.L()
		defer logger.Sync()

		var sc export.StorageConfig
		sc.URL = viper.GetString("export.storage.url")
		if sc.URL == "" {
			return errors.New("missing storage url")
		}
		sc.Username = viper.GetString("export.storage.username")
		sc.Password = viper.GetString("export.storage.password")

		return export.ExportAll(logger, sc)
	},
}

func init() {
	exportCmd.Flags().String("storage-url", "", "URL of the WebDAV server where data files are stored")
	exportCmd.Flags().String("storage-username", "", "Username")
	exportCmd.Flags().String("storage-password", "", "Password")
	viper.BindPFlag("export.storage.url", exportCmd.Flags().Lookup("storage-url"))
	viper.BindPFlag("export.storage.username", exportCmd.Flags().Lookup("storage-username"))
	viper.BindPFlag("export.storage.password", exportCmd.Flags().Lookup("storage-password"))
}
