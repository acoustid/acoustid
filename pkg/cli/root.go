package cli

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"log"
	"strings"
)

var configFile string

var rootCmd = &cobra.Command{
	Use:           "acoustid",
	Short:         "AcoustID is an audio identification service",
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file")
	rootCmd.AddCommand(dataCmd)
}

func initConfig(logger *zap.Logger) {
	viper.SetEnvPrefix("ASERVER")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if configFile != "" {
		viper.SetConfigFile(configFile)
		err := viper.ReadInConfig()
		if err != nil {
			logger.Fatal("Failed to read config file", zap.Error(err))
		}
		logger.Info("Using config file", zap.String("config", viper.ConfigFileUsed()))
	}
}

func Run() {
	loggingConfig := zap.NewDevelopmentConfig()
	loggingConfig.DisableStacktrace = true
	logger, err := loggingConfig.Build()
	if err != nil {
		log.Fatalf("Can't initialize zap logger: %v", err)
	}
	defer logger.Sync()

	undoRedirectStdLog := zap.RedirectStdLog(logger)
	defer undoRedirectStdLog()

	undoReplaceGlobals := zap.ReplaceGlobals(logger)
	defer undoReplaceGlobals()

	cobra.OnInitialize(func() { initConfig(logger) })

	if err := rootCmd.Execute(); err != nil {
		logger.Fatal(err.Error())
	}
}
