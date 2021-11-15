package standalone_storage

import (
	"os"
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kv *badger.DB
}

type StandAloneStorageReader struct {
	tx *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.tx, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.tx)
}

func (s *StandAloneStorageReader) Close() {
	s.tx.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := filepath.Join(conf.DBPath, "kv")
	if err := os.MkdirAll(kvPath, os.ModePerm); err != nil {
		return nil
	}
	ss := &StandAloneStorage{kv: engine_util.CreateDB(kvPath, conf.Raft)}
	return ss
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	tx := s.kv.NewTransaction(false)
	reader := &StandAloneStorageReader{tx: tx}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if len(batch) == 0 {
		return nil
	}
	tx := s.kv.NewTransaction(true)
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			key := engine_util.KeyWithCF(modify.Cf(), modify.Key())
			if err := tx.Set(key, modify.Value()); err != nil {
				return err
			}
		case storage.Delete:
			key := engine_util.KeyWithCF(modify.Cf(), modify.Key())
			if err := tx.Delete(key); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
