package heliumhelper

// #cgo LDFLAGS: -lhe
// #include <stdlib.h>
// #include <he.h>
import "C"
import (
	"bytes"
	"errors"
	"unsafe"

	"github.com/hyperledger/fabric/common/flogging"
)

const heMaxValLen = 16777215

const heOCreate = 1
const heOTruncate = 2
const heOVolumeCreate = 4
const hEOVolumeTruncate = 8

const heErrItemNotFound = -121

var logger = flogging.MustGetLogger("heliumhelper")

type Provider struct {
	heURL string
}

func NewProvider(heURL string) *Provider {
	return &Provider{heURL}
}

func (p *Provider) GetDSHandle(dbName string) *HeDatastore {
	logger.Debugf("GetDSHandle called; dbName: %s", dbName)
	// Open a separate datastore for each db
	c_url := C.CString(p.heURL)
	c_name := C.CString(dbName)
	c_flags := C.int(heOCreate | heOVolumeCreate) // TODO: Put this in configs

	defer C.free(unsafe.Pointer(c_url))
	defer C.free(unsafe.Pointer(c_name))

	handle := C.he_open(c_url, c_name, c_flags, nil) // TODO: Get env from configs

	return &HeDatastore{handle}
}

type HeDatastore struct {
	handle C.he_t
}

func (ds *HeDatastore) Put(key []byte, val []byte) error {
	logger.Debugf("ds.Put called; key size: %d, val size: %d", len(key), len(val))
	c_key := C.CBytes(key)
	c_val := C.CBytes(val)
	key_len := C.size_t(len(key))
	val_len := C.size_t(len(val))

	defer C.free(c_key)
	defer C.free(c_val)

	item := C.struct_he_item{c_key, c_val, key_len, val_len}

	ret := C.he_update(ds.handle, &item)

	if int(ret) != 0 {
		return errors.New("Failed to put item")
	}

	return nil
}

func (ds *HeDatastore) Get(key []byte) ([]byte, error) {
	logger.Debugf("ds.Get called; key size: %d", len(key))
	c_key := C.CBytes(key)
	key_len := C.size_t(len(key))

	defer C.free(c_key)

	// Use an initial guess for the val_len
	val_len := C.size_t(2048) // TODO: Move initial buffer size to config
	c_val := C.malloc(val_len)

	item := C.struct_he_item{key: c_key, val: c_val, key_len: key_len}

	err := C.he_lookup(ds.handle, &item, C.size_t(0), val_len)

	// If there was an error looking up the key (for example, item not found),
	// return nil for byte array
	if int(err) != 0 {
		C.free(c_val)
		// If key was not found, don't return error
		if int(err) == heErrItemNotFound {
			logger.Debugf("Key not found: %x", key)
			return nil, nil
		}
		// If any other error occurs, return error
		return nil, errors.New("Failed to lookup key")
	}

	if int(item.val_len) > int(val_len) {
		// Item value size was larger than initial buffer size; reallocate buffer and retry
		logger.Infof("Initial val_len guess was too small: %d < %d", int(val_len), int(item.val_len))
		C.free(c_val)
		val_len = item.val_len
		c_val = C.malloc(val_len)

		item.val = c_val

		C.he_lookup(ds.handle, &item, C.size_t(0), item.val_len)
	}

	val_len = item.val_len

	val := C.GoBytes(c_val, C.int(val_len))

	C.free(c_val)

	return val, nil
}

func (ds *HeDatastore) Close() {
	logger.Debugf("ds.Close called")
	C.he_close(ds.handle)
}

type HeTransaction struct {
	handle C.he_t
}

func (ds *HeDatastore) NewTransaction() (*HeTransaction, error) {
	logger.Debugf("Creating new transaction")
	txHandle := C.he_transaction(ds.handle, C.uint16_t(0)) // Don't set any tx flags

	if txHandle == nil {
		return nil, errors.New("Failed to create new transaction")
	}

	return &HeTransaction{txHandle}, nil
}

func (tx *HeTransaction) Put(key []byte, val []byte) error {
	logger.Debugf("tx.Put called; key size: %d, val size: %d", len(key), len(val))
	c_key := C.CBytes(key)
	c_val := C.CBytes(val)
	key_len := C.size_t(len(key))
	val_len := C.size_t(len(val))

	defer C.free(c_key)
	defer C.free(c_val)

	item := C.struct_he_item{c_key, c_val, key_len, val_len}

	ret := C.he_update(tx.handle, &item)

	if int(ret) != 0 {
		return errors.New("Failed to add item put to transaction")
	}

	return nil
}

func (tx *HeTransaction) Delete(key []byte) error {
	logger.Debugf("tx.Delete called; key size: %d", len(key))
	c_key := C.CBytes(key)
	key_len := C.size_t(len(key))

	defer C.free(c_key)

	item := C.struct_he_item{key: c_key, key_len: key_len}

	ret := C.he_delete(tx.handle, &item)

	if int(ret) != 0 {
		return errors.New("Failed to add item deletion to transaction")
	}

	return nil
}

func (tx *HeTransaction) Discard() error {
	logger.Debugf("tx.Discard called")
	ret := C.he_discard(tx.handle)

	if int(ret) != 0 {
		return errors.New("Failed to discard transaction")
	}

	return nil
}

func (tx *HeTransaction) Commit() error {
	logger.Debugf("tx.Commit called")
	ret := C.he_commit(tx.handle)

	if int(ret) != 0 {
		return errors.New("Failed to commit transaction")
	}

	return nil
}

type HeIterator struct {
	endKey []byte
	handle C.he_iter_t
}

// Get iterator that iterates through range [startKey, endKey)
func (ds *HeDatastore) GetIterator(startKey []byte, endKey []byte) (*HeIterator, error) {
	logger.Debugf("GetIterator called; startKey=%x; endKey=%x")
	logger.Debugf("Getting iterator for range [%#v] - [%#v]", startKey, endKey)
	c_start_key := C.CBytes(startKey)
	start_key_len := C.size_t(len(startKey))

	defer C.free(c_start_key)

	seekItem := C.struct_he_item{key: c_start_key, key_len: start_key_len}

	iter := C.he_iter_open(ds.handle, &seekItem, C.size_t(0), C.size_t(heMaxValLen), C.int(0))

	if iter == nil {
		return nil, errors.New("Failed to create iterator")
	}

	return &HeIterator{endKey, iter}, nil
}

func (iter *HeIterator) Next() ([]byte, []byte) {
	logger.Debugf("iter.Next called")
	item := C.he_iter_next(iter.handle)

	// If there is no next item in the iterator's range, return nil for key and value
	if item == nil {
		logger.Debugf("HeIterator.Next(): No more items")
		return nil, nil
	}

	key := C.GoBytes(item.key, C.int(item.key_len))

	// If the new item is outside the range (key >= endKey), return nil for key and value
	if bytes.Compare(key, iter.endKey) >= 0 {
		logger.Debugf("HeIterator.Next(): Reached item outside range")
		return nil, nil
	}

	return key, C.GoBytes(item.val, C.int(item.val_len))
}

func (iter *HeIterator) Close() {
	logger.Debugf("iter.Close called")
	C.he_iter_close(iter.handle)
}
