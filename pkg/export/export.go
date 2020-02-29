package export

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
	"io"
	"errors"
	"os"
	"strings"
	"time"
)

const PublicDataDir = "public-data"

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

func ExportFullTableFile(ctx context.Context, logger *zap.Logger, storage Storage, db *pgx.Conn, table string, columns []string) error {
	//	fileName := fmt.Sprintf("%s.full.csv.gz", table)
	//	path := storage.Join(PublicDataDir, fileName)
	return nil
}

func TableQueryBuilder(table string, columns []string) string {
	query := "SELECT"
	for i, column := range columns {
		if i > 0 {
			query += ","
		}
		query += " " + column
	}
	query += " FROM " + table
	return query
}

func TableDeltaQueryBuilder(table string, columns []string, timeColumn string, startTime time.Time, endTime time.Time) string {
	query := (TableQueryBuilder(table, columns) +
		" WHERE " +
		timeColumn + " = '" + startTime.Format(time.RFC3339) + "'" +
		" AND " +
		timeColumn + " = '" + endTime.Format(time.RFC3339) + "'")
	return query
}

type exporterTableInfo struct {
	name  string
	query string
	delta bool
}

type exporter struct {
	logger  *zap.Logger
	db      *pgx.Conn
	storage Storage
	tables  []exporterTableInfo
}

func (ex *exporter) AddTable(name string, query string, delta bool) {
	ex.tables = append(ex.tables, exporterTableInfo{name: name, query: query, delta: delta})
}

func (ex *exporter) ensurePublicDataDirExists() error {
	info, err := ex.storage.Stat(PublicDataDir)
	if err != nil {
		if err == os.ErrNotExist {
			err = ex.storage.Mkdir(PublicDataDir)
			if err != nil {
				ex.logger.Error("Failed to create public data directory", zap.String("path", PublicDataDir), zap.Error(err))
				return err
			}
		} else {
			ex.logger.Error("Failed to check if public data directory exists", zap.String("path", PublicDataDir), zap.Error(err))
			return err
		}
	}
	if !info.IsDir() {
		ex.logger.Error("The public data location exists, but is not a directory", zap.String("path", PublicDataDir))
		return errors.New("not a directory")
	}
	return nil
}

func (ex *exporter) ExportQuery(ctx context.Context, path string, query string) error {
	tmpPath := fmt.Sprintf(".%s.tmp", path)

	file, err := ex.storage.Create(tmpPath)
	if err != nil {
		ex.logger.Error("Failed to create file", zap.String("path", path), zap.Error(err))
		return err
	}
	defer file.Close()

	gzipFile := gzip.NewWriter(file)
	defer gzipFile.Close()

	copyQuery := fmt.Sprintf("COPY (%s) TO STDOUT", query)
	_, err = ex.db.PgConn().CopyTo(ctx, gzipFile, copyQuery)
	if err != nil {
		ex.logger.Error("Failed to export file", zap.String("path", path), zap.Error(err))
		return err
	}

	err = ex.storage.Rename(tmpPath, path)
	if err != nil {
		ex.logger.Error("Failed to rename exported file", zap.String("path", path), zap.Error(err))
		return err
	}

	return nil
}

func (ex *exporter) ExportTable(name string, query string, delta bool) error {
	files, err := ex.storage.ReadDir(PublicDataDir)
	if err != nil {
		return err
	}

	fileNamePrefix := name + "."
	if delta {
		fileNamePrefix += "delta."
	}

	var lastCreatedAt time.Time
	for _, file := range files {
		fileName := file.Name()
		path := ex.storage.Join(PublicDataDir, fileName)
		if strings.HasPrefix(fileName, fileNamePrefix) {
			parts := strings.SplitN(fileName, ".", 2)
			if len(parts) != 2 {
				ex.logger.Warn("Could not parse file name, unexpected number of dots", zap.String("path", path))
				continue
			}
			createdAt, err := time.Parse("2006-01", parts[2])
			if err != nil {
				ex.logger.Warn("Could not parse file name, invalid date format", zap.String("path", path), zap.Error(err))
				continue
			}
			if createdAt.After(lastCreatedAt) {
				lastCreatedAt = createdAt
			}
		}
	}

	now := time.Now()

	if delta {
		if lastCreatedAt.IsZero() {
			lastCreatedAt = time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
		}
	} else {
		if !lastCreatedAt.AddDate(0, 1, 0).After(now) {
			fileName := fmt.Sprintf("%s.full.%s.csv.gz", name, now.Format("2006-01"))
			path := ex.storage.Join(PublicDataDir, fileName)
			err = ex.ExportQuery(context.Background(), path, query)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ex *exporter) Run() error {
	err := ex.ensurePublicDataDirExists()
	if err != nil {
		return err
	}

	for _, table := range ex.tables {
		err = ex.ExportTable(table.name, table.query, table.delta)
		if err != nil {
			return err
		}
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

	ex := &exporter{db: db, storage: storage, logger: logger}
	ex.AddTable("fingerprint", ExportFingerprintDeltaQuery, true)
	return ex.Run()
}
