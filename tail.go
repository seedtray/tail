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
	// Cursor to next read
	offset int64
}

type Record struct {
	id   RecordID
	line []byte
}

type RecordBatch struct {
	id    RecordID
	lines [][]byte
}

func (b *RecordBatch) append(line []byte, id RecordID) {
	b.lines = append(b.lines, line)
	b.id = id
}

func (b *RecordBatch) len() int {
	return len(b.lines)
}

type Tailer interface {
	tail(ctx context.Context, dirname string, offset RecordID) <-chan RecordBatch
}

type FSReader struct {
	// TODO: Remove and move as a parameter
	batchSize  int
	chunkCache TailChunks
	cursor     *RecordID
}

func (r *FSReader) findChunks(dirname TailerPath, to *RecordID) error {
	var err error
	if r.chunkCache, err = dirname.filenames(); err != nil {
		return err
	}
	if to != nil {
		r.chunkCache = r.chunkCache.chunkSeek(*to)
	}
	return nil
}

func (r *FSReader) singleCatchup(
	fName string,
	offset int64,
	maxLines int,
	buffer *RecordBatch,
) error {
	f, err := defaultFS.Open(fName)
	if err != nil {
		return fmt.Errorf("could not open file %q: %s", fName, err)
	}
	defer f.Close()
	f.Seek(offset, 0)
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("could not read from file: %s", err)
		}
		offset += int64(len(line))
		buffer.append(line, RecordID{
			filename: filepath.Base(f.Name()),
			offset:   offset,
		})
		if err == io.EOF {
			return io.EOF
		}
		if buffer.len() == maxLines {
			return nil
		}
	}
}

func (r *FSReader) tail(
	ctx context.Context,
	dirname string,
	from *RecordID,
) (<-chan *RecordBatch, error) {
	// Prep list of files needed for catchup
	if err := r.findChunks(TailerPath(dirname), from); err != nil {
		return nil, err
	}

	var output = make(chan *RecordBatch)
	go func() {
		// Catchup phase (historic records)
		batch := &RecordBatch{}
		for i, fName := range r.chunkCache {
			var nextReadOffset int64 = 0
			// NOTE: skip until *from for first chunk
			if i == 0 && from != nil {
				nextReadOffset = from.offset
			}
			for {
				err := r.singleCatchup(fName, nextReadOffset, r.batchSize, batch)
				if err != nil && err != io.EOF {
					close(output)
					return
				}
				nextReadOffset = batch.id.offset
				// Flush batch
				if batch.len() >= r.batchSize {
					select {
					case <-ctx.Done():
						return
					case output <- batch:
					}
					batch = &RecordBatch{}
				}
				if err == io.EOF {
					break
				}
			}
		}
		// TODO: continuar el tail con inotify / FS watch / timed poll
		// Final flush
		select {
		case <-ctx.Done():
			return
		case output <- batch:
		}
		close(output)
	}()

	return output, nil
}

type TailerPath string

func (t TailerPath) filenames() (TailChunks, error) {
	files, err := afero.ReadDir(defaultFS, string(t))
	if err != nil {
		return nil, err
	}
	var filenames []string
	for _, f := range files {
		filenames = append(filenames, path.Join(string(t), f.Name()))
	}
	// NOTE: Sort order defines record order across file boundaries
	sort.Strings(filenames)
	return filenames, nil
}

type TailChunks []string

func (chunks TailChunks) chunkSeek(from RecordID) TailChunks {
	for k, v := range chunks {
		if filepath.Base(v) == from.filename {
			return chunks[k:]
		}
	}
	return nil
}
