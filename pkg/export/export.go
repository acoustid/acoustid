package export

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/studio-b12/gowebdav"
	"go.uber.org/zap"
	"log"
	"sort"
	"strings"
)

func ExportFingerprintDelta(logger *zap.Logger, db *pgx.Conn) error {
	db.PgConn().CopyTo(context.Background(), w, "COPY () TO STDOUT")

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

	files, err := storage.ReadDir("/public-data")
	if err != nil {
		return err
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	for _, file := range files {
		log.Println(file)
		if strings.HasSuffix(file.Name(), ".tmp") {
			log.Println("Remove", file.Name())
			//				storage.Remove(file)
		}
	}

	return nil
}
