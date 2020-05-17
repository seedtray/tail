package tail

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/spf13/afero"
)

func TestTail(t *testing.T) {
	g := NewGomegaWithT(t)
	defaultFS = afero.NewMemMapFs()
	reader := FSReader{batchSize: 4}

	afero.WriteFile(defaultFS, "/foo/1", []byte{1, 10, 2, 10, 3}, 644)
	afero.WriteFile(defaultFS, "/foo/2", []byte{4, 10, 5}, 644)
	afero.WriteFile(defaultFS, "/foo/3", []byte{6, 10, 7}, 644)

	b1, err := reader.tail(context.Background(), "/foo", nil)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(b1).To(Equal(&RecordBatch{
		id: RecordID{
			filename: "2",
			offset:   2,
		},
		lines: [][]byte{
			{1, 10},
			{2, 10},
			{3},
			{4, 10},
		},
	}))

	b2, err := reader.tail(context.Background(), "/foo", &b1.id)
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(b2).To(Equal(&RecordBatch{
		id: RecordID{
			filename: "3",
			offset:   3,
		},
		lines: [][]byte{
			{5},
			{6, 10},
			{7},
		},
	}))

}
