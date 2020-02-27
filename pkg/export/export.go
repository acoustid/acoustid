package export

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
	"os"
	"io"
	"sort"
	"strings"
	"time"
)

func ExportQuery(ctx context.Context, db *pgx.Conn, writer io.Writer, query string) error {
	copyQueryTmpl := "COPY (%s) TO STDOUT"
	copyQuery := fmt.Sprintf(copyQueryTmpl, query)
	_, err := db.PgConn().CopyTo(ctx, writer, copyQuery)
	return err
}

func ExportFingerprintDelta(ctx context.Context, db *pgx.Conn, writer io.Writer, startTime, endTime time.Time) error {
	startTimeStr, err := db.PgConn().EscapeString(startTime.Format(time.RFC3339))
	if err != nil {
		return err
	}
	endTimeStr, err := db.PgConn().EscapeString(endTime.Format(time.RFC3339))
	if err != nil {
		return err
	}
	queryTmpl := "SELECT 'I' AS op, id, length, fingerprint, created FROM fingerprint WHERE created >= '%s' AND created < '%s'"
	query := fmt.Sprintf(queryTmpl, startTimeStr, endTimeStr)
	return ExportQuery(ctx, db, writer, query)
}

func ExportFingerprintDeltaFile(ctx context.Context, logger *zap.Logger, storage Storage, db *pgx.Conn, startTime, endTime time.Time) error {
	logger.Info("Exporting fingerprint delta data file", zap.Time("start", startTime), zap.Time("end", endTime))

	fileName := fmt.Sprintf("fingerprint.delta.%s.csv.gz", startTime.Format("2006-01"))
	path := storage.Join("public-data", fileName)

	file, err := storage.Create(path)
	if err != nil {
		logger.Error("Failed to create file", zap.String("path", path), zap.Error(err))
		return err
	}
	defer file.Close()

	gzipFile := gzip.NewWriter(file)
	defer gzipFile.Close()

	err = ExportFingerprintDelta(ctx, db, gzipFile, startTime, endTime)
	if err != nil {
		logger.Error("Failed to export file", zap.String("path", path), zap.Error(err))
		return err
	}

	return nil
}

func ExportFingerprintDeltaFiles(ctx context.Context, logger *zap.Logger, storage Storage, db *pgx.Conn, totalStartTime, totalEndTime time.Time) error {
	startTime := totalStartTime
	endTime := startTime.AddDate(0, 1, 0)
	for !endTime.After(totalEndTime) {
		err := ExportFingerprintDeltaFile(ctx, logger, storage, db, startTime, endTime)
		if err != nil {
			return err
		}
		startTime = endTime
		endTime = startTime.AddDate(0, 1, 0)
	}
	return nil
}

func ExportAll(logger *zap.Logger, sc StorageConfig, databaseConfig *pgx.ConnConfig) error {
	storage, err := NewStorageClient(logger, sc)
	if err != nil {
		return err
	}
	defer storage.Close()

	db, err := pgx.ConnectConfig(context.Background(), databaseConfig)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())

	err = db.Ping(context.Background())
	if err != nil {
		return err
	}

	files, err := storage.ReadDir("public-data")
	if err != nil {
		if err == os.ErrNotExist {
			err = storage.MkdirAll("public-data")
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	var latestFingerprintDeltaStartTime time.Time = time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, file := range files {
		name := file.Name()
		path := storage.Join("public-data", name)
		if strings.HasSuffix(name, ".tmp") {
			logger.Info("Removing incomplete data file", zap.String("op", "delete"), zap.String("path", path))
			storage.Remove(path)
		} else if strings.HasSuffix(name, ".csv.gz") {
			if strings.HasPrefix(name, "fingerprint.delta.") {
				parts := strings.Split(name, ".")
				if len(parts) != 5 {
					logger.Warn("Could not parse file name, unexpected number of dots", zap.String("path", path))
					continue
				}
				startTime, err := time.Parse("2006-01", parts[2])
				if err != nil {
					logger.Warn("Could not parse file name, invalid date format", zap.String("path", path), zap.Error(err))
					continue
				}
				endTime := startTime.AddDate(0, 1, 0)
				if endTime.After(latestFingerprintDeltaStartTime) {
					latestFingerprintDeltaStartTime = endTime
				}
				logger.Info(
					"Found fingerprint delta data file",
					zap.String("path", path),
					zap.Int64("size", file.Size()),
					zap.Time("start", startTime),
					zap.Time("end", endTime),
				)
			}
		}
	}

	err = ExportFingerprintDeltaFiles(context.Background(), logger, storage, db, latestFingerprintDeltaStartTime, time.Now())
	if err != nil {
		return err
	}

	return nil
}
