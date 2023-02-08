package standalone_storage

import (
	"strings"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1)
	op := badger.DefaultOptions
	op.Dir = s.conf.DBPath
	op.ValueDir = s.conf.DBPath

	db, err := badger.Open(op)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func formatKey(cf string, key []byte) string {
	return strings.Join([]string{cf, string(key)}, ":")
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, m := range batch {
		var cf string
		var key, value []byte
		switch m.Data.(type) {
		case storage.Put:
			cf = m.Data.(storage.Put).Cf
			key = m.Data.(storage.Put).Key
			value = m.Data.(storage.Put).Value
		case storage.Delete:
			cf = m.Data.(storage.Delete).Cf
			key = m.Data.(storage.Delete).Key
		}

		formattedKey := []byte(formatKey(cf, key))
		if err := txn.Set(formattedKey, value); err == badger.ErrTxnTooBig {
			_ = txn.Commit()
			txn = s.db.NewTransaction(true)
			_ = txn.Set(formattedKey, value)
		}
	}
	return nil
}
