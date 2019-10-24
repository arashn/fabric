package statehelium

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/heliumhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util/helium"
)

var logger = flogging.MustGetLogger("statehelium")

var compositeKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)
var savePointKey = []byte{0x00}

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
	dbProvider *heliumhelper.Provider
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider() *VersionedDBProvider {
	logger.Debugf("NewVersionedDBProvider called")
	heliumDef := helium.GetHeliumDefinition()
	heURL := fmt.Sprintf("he://%s/%s", heliumDef.Server, heliumDef.DevicePaths)
	logger.Debugf("constructing VersionedDBProvider heURL=%s", heURL)
	dbProvider := heliumhelper.NewProvider(heURL)
	return &VersionedDBProvider{dbProvider}
}

// GetDBHandle gets the handle to a named database
func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	logger.Debugf("GetDBHandle called; dbName: %s", dbName)
	return newVersionedDB(provider.dbProvider.GetDSHandle(dbName), dbName), nil
}

// Close closes the underlying db
func (provider *VersionedDBProvider) Close() {
	logger.Debugf("provider.Close called")
	// Nothing to do (individual VersionedDB instances are closed)
}

// VersionedDB implements VersionedDB interface
type VersionedDB struct {
	ds     *heliumhelper.HeDatastore
	dbName string
}

// newVersionedDB constructs an instance of VersionedDB
func newVersionedDB(ds *heliumhelper.HeDatastore, dbName string) *VersionedDB {
	return &VersionedDB{ds, dbName}
}

// Open implements method in VersionedDB interface
func (vdb *VersionedDB) Open() error {
	logger.Debugf("vdb.Open called")
	// do nothing because shared db is used
	return nil
}

// Close implements method in VersionedDB interface
func (vdb *VersionedDB) Close() {
	logger.Debugf("vdb.Close called")
	vdb.ds.Close()
}

// ValidateKeyValue implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
	// helium supports any byte array for key and/or value (like leveldb)
	return nil
}

// BytesKeySupported implements method in VersionedDB interface
func (vdb *VersionedDB) BytesKeySupported() bool {
	return true
}

// GetState implements method in VersionedDB interface
func (vdb *VersionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	logger.Debugf("GetState(). ns=%s, key=%s", namespace, key)
	compositeKey := constructCompositeKey(namespace, key)
	dbVal, err := vdb.ds.Get(compositeKey)
	if err != nil {
		return nil, err
	}
	if dbVal == nil {
		return nil, nil
	}
	return decodeValue(dbVal)
}

// GetVersion implements method in VersionedDB interface
func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	versionedValue, err := vdb.GetState(namespace, key)
	if err != nil {
		return nil, err
	}
	if versionedValue == nil {
		return nil, nil
	}
	return versionedValue.Version, nil
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	return vdb.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, nil)
}

const optionLimit = "limit"

// GetStateRangeScanIteratorWithMetadata implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	logger.Debugf("GetStateRangeScanIteratorWithMetadata called; namespace=%s, startKey=%s, endKey=%s", namespace, startKey, endKey)
	requestedLimit := int32(0)
	// if metadata is provided, validate and apply options
	if metadata != nil {
		//validate the metadata
		err := statedb.ValidateRangeMetadata(metadata)
		if err != nil {
			return nil, err
		}
		if limitOption, ok := metadata[optionLimit]; ok {
			requestedLimit = limitOption.(int32)
		}
	}

	// Note:  metadata is not used for the goleveldb implementation of the range query
	compositeStartKey := constructCompositeKey(namespace, startKey)
	compositeEndKey := constructCompositeKey(namespace, endKey)
	if endKey == "" {
		// Update last byte in compositeEndKey (which is compositeKeySep) so that iterator goes through to the end of namespace
		compositeEndKey[len(compositeEndKey)-1] = lastKeyIndicator
	}
	dbItr, err := vdb.ds.GetIterator(compositeStartKey, compositeEndKey)

	if err != nil {
		return nil, err
	}

	return newResultsIterator(namespace, dbItr, requestedLimit), nil

}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return nil, errors.New("ExecuteQuery not supported for helium")
}

// ExecuteQueryWithMetadata implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("ExecuteQueryWithMetadata not supported for helium")
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *VersionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	tx, err := vdb.ds.NewTransaction()

	if err != nil {
		return err
	}

	namespaces := batch.GetUpdatedNamespaces()
	for _, ns := range namespaces {
		updates := batch.GetUpdates(ns)
		for k, vv := range updates {
			compositeKey := constructCompositeKey(ns, k)
			logger.Debugf("Channel [%s]: Applying key(string)=[%s] key(bytes)=[%#v]", vdb.dbName, string(compositeKey), compositeKey)

			if vv.Value == nil {
				tx.Delete(compositeKey)
			} else {
				encodedVal, err := encodeValue(vv)
				if err != nil {
					return err
				}
				tx.Put(compositeKey, encodedVal)
			}
		}
	}
	// Record a savepoint at a given height
	// If a given height is nil, it denotes that we are committing pvt data of old blocks.
	// In this case, we should not store a savepoint for recovery. The lastUpdatedOldBlockList
	// in the pvtstore acts as a savepoint for pvt data.
	if height != nil {
		tx.Put(savePointKey, height.ToBytes())
	}
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {
	versionBytes, err := vdb.ds.Get(savePointKey)
	if err != nil {
		return nil, err
	}
	if versionBytes == nil {
		return nil, nil
	}
	version, _, err := version.NewHeightFromBytes(versionBytes)
	if err != nil {
		return nil, err
	}
	return version, nil
}

func constructCompositeKey(ns string, key string) []byte {
	return append(append([]byte(ns), compositeKeySep...), []byte(key)...)
}

func splitCompositeKey(compositeKey []byte) (string, string) {
	split := bytes.SplitN(compositeKey, compositeKeySep, 2)
	return string(split[0]), string(split[1])
}

// ResultsIterator implements interface ResultsIterator
type ResultsIterator struct {
	namespace            string
	dbItr                *heliumhelper.HeIterator
	requestedLimit       int32
	totalRecordsReturned int32
}

func newResultsIterator(namespace string, dbItr *heliumhelper.HeIterator, requestedLimit int32) *ResultsIterator {
	return &ResultsIterator{namespace, dbItr, requestedLimit, 0}
}

// Next implements method in ResultsIterator interface
func (iterator *ResultsIterator) Next() (statedb.QueryResult, error) {
	if iterator.requestedLimit > 0 && iterator.totalRecordsReturned >= iterator.requestedLimit {
		return nil, nil
	}

	dbKey, dbVal := iterator.dbItr.Next()
	if dbKey == nil {
		return nil, nil
	}
	dbValCopy := make([]byte, len(dbVal))
	copy(dbValCopy, dbVal)
	_, key := splitCompositeKey(dbKey)
	vv, err := decodeValue(dbValCopy)
	if err != nil {
		return nil, err
	}

	iterator.totalRecordsReturned++

	return &statedb.VersionedKV{
		CompositeKey: statedb.CompositeKey{Namespace: iterator.namespace, Key: key},
		// TODO remove dereferrencing below by changing the type of the field
		// `VersionedValue` in `statedb.VersionedKV` to a pointer
		VersionedValue: *vv}, nil
}

// Close implements method in ResultsIterator interface
func (iterator *ResultsIterator) Close() {
	logger.Debugf("Close() called")
	iterator.dbItr.Close()
}

// GetBookmarkAndClose implements method in ResultsIterator interface
func (iterator *ResultsIterator) GetBookmarkAndClose() string {
	logger.Debugf("GetBookmarkAndClose() called")
	retval := ""
	dbKey, _ := iterator.dbItr.Next()
	if dbKey != nil {
		_, key := splitCompositeKey(dbKey)
		retval = key
	}
	iterator.Close()
	return retval
}
