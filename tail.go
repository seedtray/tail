package tail

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"

	"github.com/spf13/afero"
)

var defaultFS = afero.NewOsFs()

type RecordID struct {
	filename string
	offset   int64
}

type Record struct {
	id   RecordID
	line []byte
}

type RecordBatch struct {
	id    RecordID
	lines [][]byte
}

type Reader interface {
	tail(dirname string, offset RecordID) <-chan RecordBatch
}

type FSReader struct {
	// TODO: Remove and move as a parameter
	batchSize int
}

func (r FSReader) tail(ctx context.Context, dirname string, from *RecordID) (*RecordBatch, error) {
	filenames, err := filenames(dirname)
	if err != nil {
		return nil, err
	}

	if from != nil {
		filenames = filter(filenames, *from)
	}

	batch := &RecordBatch{}
	for i, fName := range filenames {
		var offset int64
		f, err := defaultFS.Open(fName)
		if err != nil {
			return nil, fmt.Errorf("could not open file %q: %s", fName, err)
		}
		if i == 0 && from != nil {
			f.Seek(from.offset, 0)
			offset = from.offset
		}
		br := bufio.NewReader(f)
		for {
			line, err := br.ReadBytes('\n')
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("could not read from file: %s", err)
			}
			offset = offset + int64(len(line))
			batch.append(line, RecordID{
				filename: filepath.Base(f.Name()),
				offset:   offset,
			})
			if batch.len() == r.batchSize {
				return batch, nil
			}
			if err == io.EOF {
				break
			}
		}
	}
	return batch, nil
}

func (b *RecordBatch) append(line []byte, id RecordID) {
	b.lines = append(b.lines, line)
	b.id = id
}

func (b *RecordBatch) len() int {
	return len(b.lines)
}

func filenames(dirname string) ([]string, error) {
	files, err := afero.ReadDir(defaultFS, dirname)
	if err != nil {
		return nil, err
	}
	var filenames []string
	for _, f := range files {
		filenames = append(filenames, path.Join(dirname, f.Name()))
	}
	sort.Strings(filenames)
	return filenames, nil
}

func filter(filenames []string, from RecordID) []string {
	for k, v := range filenames {
		if filepath.Base(v) == from.filename {
			return filenames[k:]
		}
	}
	return nil
}
