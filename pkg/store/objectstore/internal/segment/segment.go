package segment

import (
	"os"
	"path/filepath"
)

type Writer struct {
	file       *os.File
	activePath string
	sealedPath string
	size       int64
}

func Open(root, shard, segmentID string) (*Writer, error) {
	activeDir := filepath.Join(root, "ingest", "active", shard)
	sealedDir := filepath.Join(root, "ingest", "sealed", shard)
	if err := os.MkdirAll(activeDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(sealedDir, 0o755); err != nil {
		return nil, err
	}

	activePath := filepath.Join(activeDir, segmentID+".seg")
	file, err := os.Create(activePath)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:       file,
		activePath: activePath,
		sealedPath: filepath.Join(sealedDir, segmentID+".seg"),
	}, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *Writer) Seal() (string, int64, error) {
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return "", 0, err
	}
	if err := w.file.Close(); err != nil {
		return "", 0, err
	}
	if err := os.Rename(w.activePath, w.sealedPath); err != nil {
		return "", 0, err
	}
	if err := syncDir(filepath.Dir(w.sealedPath)); err != nil {
		return "", 0, err
	}
	return w.sealedPath, w.size, nil
}

func (w *Writer) Abort() error {
	closeErr := w.file.Close()
	removeErr := os.Remove(w.activePath)
	if os.IsNotExist(removeErr) {
		removeErr = nil
	}
	if removeErr == nil {
		if err := syncDir(filepath.Dir(w.activePath)); err != nil {
			removeErr = err
		}
	}
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
