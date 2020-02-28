package export

import (
	"io"
	"net"
	"strconv"
	"os"
	"github.com/pkg/sftp"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

type StorageFile interface {
	io.Reader
	io.ReaderFrom
	io.Writer
	io.WriterTo
	io.Seeker
	io.Closer
}

type Storage interface {
	Stat(path string) (os.FileInfo, error)
	ReadDir(path string) ([]os.FileInfo, error)
	Open(path string) (StorageFile, error)
	Create(path string) (StorageFile, error)
	Mkdir(path string) error
	MkdirAll(path string) error
	Remove(path string) error
	Join(elem ...string) string
}

type StorageConfig struct {
	Host     string
	Port     int
	Path     string
	User     string
	Password string
}

type StorageClient struct {
	config *StorageConfig
	client *sftp.Client
	logger *zap.Logger
}

func (c *StorageClient) Close() error {
	if c.client != nil {
		err := c.client.Close()
		if err != nil {
			return err
		}
		c.client = nil
	}
	return nil
}

func (c *StorageClient) Stat(path string) (os.FileInfo, error) {
	return c.client.Stat(sftp.Join(c.config.Path, path))
}

func (c *StorageClient) ReadDir(path string) ([]os.FileInfo, error) {
	return c.client.ReadDir(sftp.Join(c.config.Path, path))
}

func (c *StorageClient) Open(path string) (StorageFile, error) {
	return c.client.Open(sftp.Join(c.config.Path, path))
}

func (c *StorageClient) Create(path string) (StorageFile, error) {
	return c.client.Create(sftp.Join(c.config.Path, path))
}

func (c *StorageClient) Mkdir(path string) error {
	return c.client.Mkdir(sftp.Join(c.config.Path, path))
}

func (c *StorageClient) MkdirAll(path string) error {
	return c.client.MkdirAll(sftp.Join(c.config.Path, path))
}

func (c *StorageClient) Remove(path string) error {
	return c.client.Remove(sftp.Join(c.config.Path, path))
}

func (c *StorageClient) Join(elem ...string) string {
	return sftp.Join(elem...)
}

func NewStorageClient(logger *zap.Logger, sc StorageConfig) (*StorageClient, error) {
	config := &ssh.ClientConfig{
		User: sc.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(sc.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	logger.Debug("Connecting to SFTP server", zap.String("host", sc.Host), zap.Int("port", sc.Port), zap.String("user", sc.User))

	addr := net.JoinHostPort(sc.Host, strconv.Itoa(sc.Port))
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		logger.Error("Failed to connect to SSH host", zap.Error(err))
		return nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		logger.Error("Failed to open SFTP session", zap.Error(err))
		return nil, err
	}

	return &StorageClient{config: &sc, client: client, logger: logger}, nil
}
