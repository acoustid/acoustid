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
	config.LogLevel = pgx.LogLevelDebug
	config.Logger = zapadapter.NewLogger(logger.With(zap.String("database", "fingerprint")))
	return config, nil
}

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

		db, err := BuildDatabaseConfig(logger, "database.fingerprint.")
		if err != nil {
			return err
		}

		return export.ExportAll(logger, sc, db)
	},
}

func init() {
	exportCmd.Flags().String("storage-url", "", "URL of the WebDAV server where data files are stored")
	exportCmd.Flags().String("storage-username", "", "Username")
	exportCmd.Flags().String("storage-password", "", "Password")

	viper.BindPFlag("export.storage.url", exportCmd.Flags().Lookup("storage-url"))
	viper.BindPFlag("export.storage.username", exportCmd.Flags().Lookup("storage-username"))
	viper.BindPFlag("export.storage.password", exportCmd.Flags().Lookup("storage-password"))

	exportCmd.Flags().String("database-host", "127.0.0.1", "PostgreSQL host")
	exportCmd.Flags().Int("database-port", 5432, "PostgreSQL port")
	exportCmd.Flags().String("database-name", "", "PostgreSQL name")
	exportCmd.Flags().String("database-username", "", "PostgreSQL username")
	exportCmd.Flags().String("database-password", "", "PostgreSQL password")

	viper.BindPFlag("database.fingerprint.host", exportCmd.Flags().Lookup("database-host"))
	viper.BindPFlag("database.fingerprint.port", exportCmd.Flags().Lookup("database-port"))
	viper.BindPFlag("database.fingerprint.name", exportCmd.Flags().Lookup("database-name"))
	viper.BindPFlag("database.fingerprint.username", exportCmd.Flags().Lookup("database-username"))
	viper.BindPFlag("database.fingerprint.password", exportCmd.Flags().Lookup("database-password"))
}
