package cli

import (
	"errors"
	"github.com/acoustid/acoustid/pkg/export"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zapadapter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func BuildDatabaseConfig(logger *zap.Logger, prefix string) (*pgx.ConnConfig, error) {
	config, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}
	config.Host = viper.GetString(prefix + "host")
	config.Port = uint16(viper.GetInt(prefix + "port"))
	config.Database = viper.GetString(prefix + "name")
	config.User = viper.GetString(prefix + "user")
	config.Password = viper.GetString(prefix + "password")
	config.PreferSimpleProtocol = true
	config.LogLevel = pgx.LogLevelDebug
	config.Logger = zapadapter.NewLogger(logger)
	return config, nil
}

func BuildStorageConfig(logger *zap.Logger) (*export.StorageConfig, error) {
	var config export.StorageConfig
	config.Host = viper.GetString("export.storage.host")
	if config.Host == "" {
		return nil, errors.New("missing storage host")
	}
	config.Port = viper.GetInt("export.storage.port")
	config.Path = viper.GetString("export.storage.path")
	config.User = viper.GetString("export.storage.user")
	config.Password = viper.GetString("export.storage.password")
	return &config, nil
}

var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "Commands for working with public data files",
}

var dataProxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: "Run HTTP proxy for serving public data files",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := zap.L()
		defer logger.Sync()

		storage, err := BuildStorageConfig(logger)
		if err != nil {
			return err
		}
		logger.Info("xx", zap.String("storage", storage.Host))

		return export.RunProxy(logger, storage)
	},
}

var dataExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export database to a remote location",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := zap.L()
		defer logger.Sync()

		storage, err := BuildStorageConfig(logger)
		if err != nil {
			return err
		}

		db, err := BuildDatabaseConfig(logger, "database.fingerprint.")
		if err != nil {
			return err
		}

		return export.ExportAll(logger, *storage, db)
	},
}

func init() {
	dataCmd.AddCommand(dataExportCmd)
	dataCmd.AddCommand(dataProxyCmd)

	dataExportCmd.Flags().String("storage-host", "", "URL of the WebDAV server where data files are stored")
	dataExportCmd.Flags().Int("storage-port", 22, "")
	dataExportCmd.Flags().String("storage-path", "", "")
	dataExportCmd.Flags().String("storage-user", "", "Username")
	dataExportCmd.Flags().String("storage-password", "", "Password")

	viper.BindPFlag("export.storage.host", dataExportCmd.Flags().Lookup("storage-host"))
	viper.BindPFlag("export.storage.port", dataExportCmd.Flags().Lookup("storage-port"))
	viper.BindPFlag("export.storage.path", dataExportCmd.Flags().Lookup("storage-path"))
	viper.BindPFlag("export.storage.username", dataExportCmd.Flags().Lookup("storage-username"))
	viper.BindPFlag("export.storage.password", dataExportCmd.Flags().Lookup("storage-password"))

	dataExportCmd.Flags().String("database-host", "127.0.0.1", "PostgreSQL host")
	dataExportCmd.Flags().Int("database-port", 5432, "PostgreSQL port")
	dataExportCmd.Flags().String("database-name", "", "PostgreSQL name")
	dataExportCmd.Flags().String("database-username", "", "PostgreSQL username")
	dataExportCmd.Flags().String("database-password", "", "PostgreSQL password")

	viper.BindPFlag("database.fingerprint.host", dataExportCmd.Flags().Lookup("database-host"))
	viper.BindPFlag("database.fingerprint.port", dataExportCmd.Flags().Lookup("database-port"))
	viper.BindPFlag("database.fingerprint.name", dataExportCmd.Flags().Lookup("database-name"))
	viper.BindPFlag("database.fingerprint.username", dataExportCmd.Flags().Lookup("database-username"))
	viper.BindPFlag("database.fingerprint.password", dataExportCmd.Flags().Lookup("database-password"))
}
