package statehelium

import (
	"bytes"
	"errors"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/heliumhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
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
	heUrl := "he://.//dev/nvme0n1" // TODO: Put this in configs
	logger.Debugf("constructing VersionedDBProvider heUrl=%s", heUrl)
	dbProvider := heliumhelper.NewProvider(heUrl)
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

// BytesKeySuppoted implements method in VersionedDB interface
func (vdb *VersionedDB) BytesKeySuppoted() bool {
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
	val, ver := statedb.DecodeValue(dbVal)
	return &statedb.VersionedValue{Value: val, Version: ver}, nil
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
	compositeStartKey := constructCompositeKey(namespace, startKey)
	compositeEndKey := constructCompositeKey(namespace, endKey)
	if endKey == "" {
		// Increment last byte of composteEndKey (last byte of namespace) to iterate through the end of namespace
		compositeEndKey[len(compositeEndKey)-1] += lastKeyIndicator
	}
	dbItr := vdb.ds.GetIterator(compositeStartKey, compositeEndKey)
	return newResultsIterator(namespace, dbItr), nil
}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return nil, errors.New("ExecuteQuery not supported for helium")
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *VersionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	tx := heliumhelper.NewTransaction()
	namespaces := batch.GetUpdatedNamespaces()
	for _, ns := range namespaces {
		updates := batch.GetUpdates(ns)
		for k, vv := range updates {
			compositeKey := constructCompositeKey(ns, k)
			logger.Debugf("Channel [%s]: Applying key(string)=[%s] key(bytes)=[%#v]", vdb.dbName, string(compositeKey), compositeKey)

			if vv.Value == nil {
				tx.Delete(compositeKey)
			} else {
				tx.Put(compositeKey, statedb.EncodeValue(vv.Value, vv.Version))
			}
		}
	}
	tx.Put(savePointKey, height.ToBytes())
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
	version, _ := version.NewHeightFromBytes(versionBytes)
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
	namespace string
	dbItr     heliumhelper.HeIterator
}

func newResultsIterator(namespace string, dbItr heliumhelper.HeIterator) *ResultsIterator {
	return &ResultsIterator{namespace, dbItr}
}

func (iterator *ResultsIterator) Next() (statedb.QueryResult, error) {
	dbKey, dbVal := iterator.dbItr.Next()
	if dbKey == nil {
		return nil, nil
	}
	dbValCopy := make([]byte, len(dbVal))
	copy(dbValCopy, dbVal)
	_, key := splitCompositeKey(dbKey)
	value, version := statedb.DecodeValue(dbValCopy)
	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: iterator.namespace, Key: key},
		VersionedValue: statedb.VersionedValue{Value: value, Version: version}}, nil
}

func (iterator *ResultsIterator) Close() {
	iterator.dbItr.Close()
}
