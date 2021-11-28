package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	var path string

	flag.StringVar(&path, "path", "", "")
	flag.Parse()

	i, err := NewAncientIterator(path)
	if err != nil {
		panic(err)
	}

	i.Seek(10)
	for {
		block, err := i.Next()
		if err != nil {
			panic(err)
		}
		if block == nil {
			break
		}
		fmt.Printf("Block %d: Num transactions %d\n", block.Header.Number, len(block.Transactions))
	}

}

const indexEntrySize = int64(6)

type indexEntry struct {
	FileNum uint16 // 2 bytes
	Offset  uint32 // 4 bytes
}

func (i *indexEntry) Unmarshal(b []byte) {
	i.FileNum = binary.BigEndian.Uint16(b[:2])
	i.Offset = binary.BigEndian.Uint32(b[2:])
}

type Iterator interface {
	Seek(number uint64) bool
	Next() (*Block, error)
}

type AncientIterator struct {
	path   string
	tables ancientTables
	seek   uint64
}

type ancientTables struct {
	headers  ancientTable
	bodies   ancientTable
	receipts ancientTable
}

func NewAncientIterator(path string) (Iterator, error) {
	it := &AncientIterator{
		path:   path,
		tables: ancientTables{},
	}

	createTable := func(name string, t *ancientTable) error {
		table, err := newAncientTable(path, name)
		if err != nil {
			return fmt.Errorf("failed to create table '%s': %v", name, err)
		}
		*t = *table
		return nil
	}

	if err := createTable("headers", &it.tables.headers); err != nil {
		return nil, err
	}
	if err := createTable("bodies", &it.tables.bodies); err != nil {
		return nil, err
	}
	if err := createTable("receipts", &it.tables.receipts); err != nil {
		return nil, err
	}

	return it, nil
}

func (i *AncientIterator) Seek(number uint64) bool {
	i.seek = number
	return false
}

func (i *AncientIterator) Next() (*Block, error) {
	decode := func(obj Unmarshaler, table *ancientTable) error {
		dst := table.readData(nil, i.seek, 1)
		dst = dst[8:]

		return obj.UnmarshalRLP(dst)
	}

	header := new(Header)
	if err := decode(header, &i.tables.headers); err != nil {
		return nil, err
	}

	// body
	body := new(Body)
	if err := decode(body, &i.tables.bodies); err != nil {
		return nil, err
	}

	// receipts
	receipts := new(Receipts)

	// Build the result block object
	resp := &Block{
		Header:       header,
		Transactions: body.Transactions,
		Receipts:     *receipts,
	}

	i.seek++
	return resp, nil
}

type ancientTable struct {
	path       string
	name       string
	compressed bool

	// store the file of index and offsets
	index *os.File

	// data files
	data map[uint16]*os.File
}

func newAncientTable(path, name string) (*ancientTable, error) {
	t := &ancientTable{
		path: path,
		name: name,
		data: map[uint16]*os.File{},
	}
	err := t.checkIndex()
	if err != nil {
		return nil, err
	}

	// open index file
	if t.index, err = os.Open(t.getIndexName(t.compressed)); err != nil {
		return nil, err
	}

	// preopen all the data files
	if err := t.openDataFiles(); err != nil {
		return nil, err
	}
	return t, nil
}

func (a *ancientTable) openDataFiles() error {
	stat, err := a.index.Stat()
	if err != nil {
		return err
	}

	var firstEntry, lastEntry indexEntry
	buf := make([]byte, indexEntrySize)

	readAt := func(entry *indexEntry, pos int64) error {
		if _, err := a.index.ReadAt(buf, pos); err != nil {
			return err
		}
		entry.Unmarshal(buf)
		return nil
	}

	// read the first entry
	if err := readAt(&firstEntry, 0); err != nil {
		return err
	}
	// read last entry
	if err := readAt(&lastEntry, stat.Size()-indexEntrySize); err != nil {
		return err
	}

	// open the files
	for i := firstEntry.FileNum; i < lastEntry.FileNum; i++ {
		f, err := os.Open(a.getDataName(i, a.compressed))
		if err != nil {
			return err
		}
		a.data[i] = f
	}
	return nil
}

func (a *ancientTable) readData(dst []byte, from, count uint64) []byte {
	seek := func(num uint64) *indexEntry {
		buf := make([]byte, indexEntrySize)
		if _, err := a.index.ReadAt(buf, int64(num)*indexEntrySize); err != nil {
			panic(err)
		}

		entry := &indexEntry{}
		entry.Unmarshal(buf)
		return entry
	}

	indx := seek(from)

	to := from + count + 1
	for i := from + 1; i < to; i++ {
		nextIndx := seek(i)

		b := make([]byte, nextIndx.Offset-indx.Offset)
		_, err := a.data[indx.FileNum].ReadAt(b, int64(indx.Offset))
		if err != nil {
			panic(err)
		}

		if a.compressed {
			b, err = snappy.Decode(nil, b)
			if err != nil {
				panic(err)
			}
		}

		dst = append(dst, marshalUint64(uint64(len(b)))...)
		dst = append(dst, b...)
		indx = nextIndx
	}

	return dst
}

func (a *ancientTable) checkIndex() error {
	hasCompr, err := exists(a.getIndexName(true))
	if err != nil {
		return err
	}
	hasNormal, err := exists(a.getIndexName(false))
	if err != nil {
		return err
	}
	if !hasCompr && !hasNormal {
		return fmt.Errorf("table not found")
	}
	if hasCompr && hasNormal {
		return fmt.Errorf("both compress and uncompress index found")
	}
	a.compressed = hasCompr
	return nil
}

func (a *ancientTable) getDataName(indx uint16, compressed bool) string {
	ext := ""
	if compressed {
		ext = "cdat"
	} else {
		ext = "rdat"
	}
	return filepath.Join(a.path, fmt.Sprintf("%s.%04d.%s", a.name, indx, ext))
}

func (a *ancientTable) getIndexName(compressed bool) string {
	ext := ""
	if compressed {
		ext = "cidx"
	} else {
		ext = "ridx"
	}
	return filepath.Join(a.path, a.name+"."+ext)
}

type LevelDBIterator struct {
	db   *leveldb.DB
	iter iterator.Iterator
}

func NewLevelDBIterator(path string) (Iterator, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to load leveldb: %v", err)
	}
	it := &LevelDBIterator{
		db:   db,
		iter: db.NewIterator(nil, nil),
	}
	return it, nil
}

type Block struct {
	Header       *Header
	Transactions []*Transaction
	Receipts     Receipts
}

func (i *LevelDBIterator) decodeBlock(key, val []byte, includeReceipts bool) (*Block, error) {
	num, hash := decodeKey(key)

	// header
	header := new(Header)
	if err := header.UnmarshalRLP(val); err != nil {
		return nil, fmt.Errorf("failed to decode header: %v", err)
	}

	// body
	body := new(Body)
	bodyRaw, err := i.db.Get(blockBodyKey(num, hash), nil)
	if err != nil {
		return nil, err
	}
	if err := body.UnmarshalRLP(bodyRaw); err != nil {
		return nil, fmt.Errorf("failed to decode body: %v", err)
	}

	// receipts
	receipts := new(Receipts)
	if includeReceipts {
		receiptsRaw, err := i.db.Get(blockReceiptsKey(num, hash), nil)
		if err != nil {
			return nil, err
		}
		if err := receipts.UnmarshalRLP(receiptsRaw); err != nil {
			return nil, fmt.Errorf("failed to decode receipts: %v", err)
		}
	}

	// Build the result block object
	resp := &Block{
		Header:       header,
		Transactions: body.Transactions,
		Receipts:     *receipts,
	}
	return resp, nil
}

func (i *LevelDBIterator) Seek(number uint64) bool {
	return i.iter.Seek(headerByNumberPrefix(number))
}

func (i *LevelDBIterator) Next() (*Block, error) {
	for ok := true; ok; ok = i.iter.Next() {
		key, val := i.iter.Key(), i.iter.Value()
		if len(key) != 41 {
			continue
		}

		// the loop is only to move in case the key is not correct
		// but it does not loop on correct case
		i.iter.Next()

		fmt.Println(key, val)

		block, err := i.decodeBlock(key, val, false)
		if err != nil {
			return nil, err
		}
		return block, nil
	}
	return nil, nil
}

var (
	// headerPrefix + num (uint64 big endian) + hash -> header
	headerPrefix = []byte("h")

	// blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockBodyPrefix = []byte("b")

	// blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts
	blockReceiptsPrefix = []byte("r")
)

func marshalUint64(num uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, num)
	return buf
}

func unmarshalUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func decodeKey(b []byte) (uint64, []byte) {
	return unmarshalUint64(b[1:9]), b[9:]
}

func headerByNumberPrefix(number uint64) []byte {
	return append(headerPrefix, marshalUint64(number)...)
}

func blockBodyKey(number uint64, hash []byte) []byte {
	return append(append(blockBodyPrefix, marshalUint64(number)...), hash[:]...)
}

func blockReceiptsKey(number uint64, hash []byte) []byte {
	return append(append(blockReceiptsPrefix, marshalUint64(number)...), hash[:]...)
}

func exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}
