package export

import (
	"github.com/studio-b12/gowebdav"
	"go.uber.org/zap"
	"log"
	"sort"
	"strings"
)

func ExportAll(logger *zap.Logger, sc StorageConfig) error {
	storage := gowebdav.NewClient(sc.URL, sc.Username, sc.Password)

	files, err := storage.ReadDir("/public-data")
	if err != nil {
		return err
	}

	sort.Slice(files, func (i, j int) bool { return files[i].Name() < files[j].Name() })

	for _, file := range files {
		log.Println(file)
		if strings.HasSuffix(file.Name(), ".tmp") {
			log.Println("Remove", file.Name())
//				storage.Remove(file)
		}
	}

	return nil
}
