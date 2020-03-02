package export

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
	"text/template"
	"time"
)

const bufferSize = 1024 * 16

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

func (ex *exporter) RenderQueryTemplate(queryTmpl string, startTime, endTime time.Time) (string, error) {
	tmplCtx := QueryContext{
		StartTime: startTime.Format(time.RFC3339),
		EndTime:   endTime.Format(time.RFC3339),
	}

	tmpl, err := template.New("query").Parse(queryTmpl)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, &tmplCtx)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (ex *exporter) ExportQuery(ctx context.Context, path string, query string) error {
	tempPath := fmt.Sprintf("%s.tmp", path)

	logger := ex.logger.With(zap.String("path", path))

	file, err := ex.storage.Create(tempPath)
	if err != nil {
		logger.Error("Failed to create temporary file", zap.Error(err))
		return err
	}

	fileClosed := false
	fileRenamed := false

	cleanupFile := func() {
		if !fileClosed {
			err := file.Close()
			if err != nil {
				logger.Error("Failed to close temporary file", zap.Error(err))
			}
			fileClosed = true
		}
		if !fileRenamed {
			err := ex.storage.Remove(tempPath)
			if err != nil {
				logger.Error("Failed to delete temporary file", zap.Error(err))
			}
			fileRenamed = true
		}
	}

	defer cleanupFile()

	bufferedFile := bufio.NewWriterSize(file, bufferSize)
	gzipFile := gzip.NewWriter(bufferedFile)

	copyQuery := fmt.Sprintf("COPY (SELECT row_to_json(r) FROM (%s) r) TO STDOUT", query)
	_, err = ex.db.PgConn().CopyTo(ctx, gzipFile, copyQuery)
	if err != nil {
		logger.Error("Failed to export file", zap.Error(err))
		return err
	}

	err = gzipFile.Close()
	if err != nil {
		logger.Error("Failed to close gzip file", zap.Error(err))
		return err
	}

	err = bufferedFile.Flush()
	if err != nil {
		logger.Error("Failed to flush buffers", zap.Error(err))
		return err
	}

	err = file.Close()
	if err != nil {
		logger.Error("Failed to close file", zap.Error(err))
		return err
	}
	fileClosed = true

	err = ex.storage.Rename(tempPath, path)
	if err != nil {
		logger.Error("Failed to rename exported file", zap.String("path", path), zap.Error(err))
		return err
	}
	fileRenamed = true

	return nil
}

func (ex *exporter) ExportTableFull(now time.Time, name string, query string) error {
	return errors.New("not implemented")
}

func (ex *exporter) ExportDailyFile(name string, queryTmpl string, startTime, endTime time.Time) error {
	file := fmt.Sprintf("%s.daily.%s.jsonl.gz", name, startTime.Format("2006-01-02"))
	directory := ex.storage.Join("public-data", startTime.Format("2006"), startTime.Format("2006-01"))
	path := ex.storage.Join(directory, file)

	logger := ex.logger.With(zap.String("name", name), zap.String("path", path))
	defer logger.Sync()

	fileExists, err := CheckFileExists(ex.storage, path)
	if err != nil {
		logger.Error("Failed to check if file exists", zap.Error(err))
		return err
	}
	if fileExists {
		logger.Debug("File already exists")
		return nil
	}

	logger.Info("Exporting file")

	err = EnsureDirExists(ex.storage, directory)
	if err != nil {
		logger.Error("Failed to create parent directory", zap.Error(err))
		return err
	}

	query, err := ex.RenderQueryTemplate(queryTmpl, startTime, endTime)
	if err != nil {
		logger.Error("Failed to render query template", zap.Error(err))
		return err
	}

	err = ex.ExportQuery(context.Background(), path, query)
	if err != nil {
		logger.Error("Failed to export file", zap.Error(err))
		return err
	}

	return nil
}

func (ex *exporter) ExportDailyFiles(name string, queryTmpl string, endTime time.Time, dayCount int) error {
	endTime = time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, endTime.Location())
	for i := 0; i < dayCount; i++ {
		startTime := endTime.AddDate(0, 0, -1)
		err := ex.ExportDailyFile(name, queryTmpl, startTime, endTime)
		if err != nil {
			return err
		}
		endTime = startTime
	}
	return nil
}

func (ex *exporter) Run() error {
	now := time.Now()
	for _, table := range ex.tables {
		var err error
		if table.delta {
			err = ex.ExportDailyFiles(table.name, table.query, now, 30)
		}
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
