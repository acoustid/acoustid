package export

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/studio-b12/gowebdav"
	"go.uber.org/zap"
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

func ExportFingerprintDeltaFile(ctx context.Context, db *pgx.Conn, writer io.Writer, startTime, endTime time.Time) error {
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

func ExportFingerprintDelta(ctx context.Context, logger *zap.Logger, storage *gowebdav.Client, db *pgx.Conn, totalStartTime, totalEndTime time.Time) error {
	startTime := totalStartTime
	endTime := startTime.AddDate(0, 1, 0)
	for !endTime.After(totalEndTime) {
		logger.Info("Exporting fingerprint delta data file", zap.Time("start", startTime), zap.Time("end", endTime))

		exportResult := make(chan error, 1)

		reader, writer := io.Pipe()
		gzipWriter := gzip.NewWriter(writer)

		go func() {
			defer gzipWriter.Close()
			defer writer.Close()
			exportResult <- ExportFingerprintDeltaFile(ctx, db, gzipWriter, startTime, endTime)
		}()

		err := storage.WriteStream(fmt.Sprintf("/public-data/fingerprint.delta.%s.csv.gz", startTime.Format("2006-01")), reader, 0644)
		if err != nil {
			logger.Error("Upload failed", zap.Error(err))
			return err
		}

		err = <-exportResult
		if err != nil {
			logger.Error("Export failed", zap.Error(err))
			return err
		}

		startTime = endTime
		endTime = startTime.AddDate(0, 1, 0)
	}
	return nil
}

func ExportAll(logger *zap.Logger, sc StorageConfig, databaseConfig *pgx.ConnConfig) error {
	storage := gowebdav.NewClient(sc.URL, sc.Username, sc.Password)

	db, err := pgx.ConnectConfig(context.Background(), databaseConfig)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())

	err = db.Ping(context.Background())
	if err != nil {
		return err
	}

	publicDataPath := "/public-data/"
	files, err := storage.ReadDir(publicDataPath)
	if err != nil {
		if strings.Contains(err.Error(), "404 Not Found") {
			err = storage.MkdirAll("/public-data/", 0775)
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
		path := publicDataPath + name
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
				logger.Info("Found fingerprint delta data file", zap.String("path", path), zap.Time("start", startTime), zap.Time("end", endTime), zap.String("file", fmt.Sprintf("%v", file)))
			}
		}
	}

	err = ExportFingerprintDelta(context.Background(), logger, storage, db, latestFingerprintDeltaStartTime, time.Now())
	if err != nil {
		return err
	}

	return nil
}
