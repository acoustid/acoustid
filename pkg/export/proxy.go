package export

import (
	"go.uber.org/zap"
	"net/http"
	"os"
)

type ProxyFile struct {
	StorageFile
	storage Storage
	logger  *zap.Logger
	path    string
}

func (f *ProxyFile) Readdir(count int) ([]os.FileInfo, error) {
	f.logger.Info("readdir", zap.String("path", f.path))
	return f.storage.ReadDir(f.path)
}

func (f *ProxyFile) Stat() (os.FileInfo, error) {
	f.logger.Info("stat", zap.String("path", f.path))
	return f.storage.Stat(f.path)
}

func (f *ProxyFile) Close() error {
	if f.StorageFile != nil {
		return f.StorageFile.Close()
	}
	return nil
}

type ProxyFilesystem struct {
	storage Storage
	logger  *zap.Logger
}

func (fs *ProxyFilesystem) Open(path string) (http.File, error) {
	fs.logger.Info("open", zap.String("path", path))
	file, err := fs.storage.Open(path)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, err
		}
		fs.logger.Info("Open failed", zap.Error(err))
		info, err2 := fs.storage.Stat(path)
		if err2 != nil {
			fs.logger.Info("Stat failed", zap.Error(err2))
			return nil, err
		}
		if err2 == nil || info.IsDir() {
			return &ProxyFile{StorageFile: nil, storage: fs.storage, logger: fs.logger, path: path}, nil
		}
		return nil, err
	}
	return &ProxyFile{StorageFile: file, storage: fs.storage, logger: fs.logger, path: path}, nil
}

func RunProxy(logger *zap.Logger, sc *StorageConfig) error {
	storage, err := NewStorageClient(logger, *sc)
	if err != nil {
		return err
	}
	defer storage.Close()

	fs := &ProxyFilesystem{storage: storage, logger: logger}
	http.Handle("/", http.FileServer(fs))
	return http.ListenAndServe(":8080", nil)
}
