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
	"math/rand"
	"strings"
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
	maxDays int
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

func (ex *exporter) makeTempPath(path string) string {
	directory, fileName := ex.storage.Split(path)
	return ex.storage.Join(directory, fmt.Sprintf(".%s.%d.tmp", fileName, rand.Int()))
}

func (ex *exporter) ExportQuery(ctx context.Context, path string, query string) error {
	logger := ex.logger.With(zap.String("path", path))

	tempPath := ex.makeTempPath(path)
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

	copyQuery := fmt.Sprintf("COPY (SELECT json_strip_nulls(row_to_json(r)) FROM (%s) r) TO STDOUT", query)
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

func (ex *exporter) DeleteTempFiles(name string, startTime time.Time) error {
	fileName := fmt.Sprintf("%s-%s.jsonl.gz", startTime.Format("2006-01-02"), name)
	directory := ex.storage.Join(startTime.Format("2006"), startTime.Format("2006-01"))
	files, err := ex.storage.ReadDir(directory)
	if err != nil {
		ex.logger.Error("Failed to list files", zap.Error(err))
		return err
	}
	for _, file := range files {
		if strings.Contains(file.Name(), fileName) && strings.HasSuffix(file.Name(), ".tmp") {
			path := ex.storage.Join(directory, file.Name())
			err = ex.storage.Remove(path)
			if err != nil {
				ex.logger.Error("Failed to delete temporary file", zap.Error(err), zap.String("path", path))
			}
		}
	}
	return nil
}

func (ex *exporter) ExportDeltaFile(name string, queryTmpl string, startTime, endTime time.Time) error {
	fileName := fmt.Sprintf("%s-%s.jsonl.gz", startTime.Format("2006-01-02"), name)
	directory := ex.storage.Join(startTime.Format("2006"), startTime.Format("2006-01"))
	path := ex.storage.Join(directory, fileName)

	logger := ex.logger.With(zap.String("name", name), zap.String("path", path))
	defer logger.Sync()

	fileExists, err := CheckFileExists(ex.storage, path)
	if err != nil {
		logger.Error("Failed to check if file exists", zap.Error(err))
		return err
	}
	if fileExists {
		logger.Debug("File already exists")
	} else {
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
	}

	err = ex.DeleteTempFiles(name, startTime)
	if err != nil {
		logger.Error("Failed to delete temporary file", zap.Error(err))
		return err
	}

	return nil
}

func (ex *exporter) Run() error {
	now := time.Now()
	endTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	for i := 0; i < ex.maxDays; i++ {
		startTime := endTime.AddDate(0, 0, -1)
		for _, table := range ex.tables {
			if !table.delta {
				continue
			}
			err := ex.ExportDeltaFile(table.name, table.query, startTime, endTime)
			if err != nil {
				return err
			}
		}
		endTime = startTime
	}
	return nil
}

func ExportAll(logger *zap.Logger, sc StorageConfig, databaseConfig *pgx.ConnConfig, maxDays int) error {
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

	ex := &exporter{db: db, storage: storage, logger: logger, maxDays: maxDays}
	ex.AddTable("fingerprint-update", ExportFingerprintUpdateQuery, true)
	ex.AddTable("meta-update", ExportMetaUpdateQuery, true)
	ex.AddTable("track-update", ExportTrackUpdateQuery, true)
	ex.AddTable("track_fingerprint-update", ExportTrackFingerprintUpdateQuery, true)
	ex.AddTable("track_mbid-update", ExportTrackMbidUpdateQuery, true)
	ex.AddTable("track_puid-update", ExportTrackPuidUpdateQuery, true)
	ex.AddTable("track_meta-update", ExportTrackMetaUpdateQuery, true)
	return ex.Run()
}
