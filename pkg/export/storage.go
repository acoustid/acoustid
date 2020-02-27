package export

import (
	"io"
	"net"
	"strconv"
	"github.com/pkg/sftp"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

type StorageFile interface {
	io.Reader
	io.ReaderFrom
	io.Writer
	io.WriterTo
	io.Closer
}

type StorageConfig struct {
	Host     string
	Port     int
	Path     string
	User     string
	Password string
}

type StorageClient struct {
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

func NewStorageClient(logger *zap.Logger, sc StorageConfig) (*sftp.Client, error) {
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

	return client, nil
}
