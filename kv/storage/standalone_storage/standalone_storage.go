package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db      *badger.DB                 // data base
	readers []*StandAloneStorageReader // readers of current storage, must close when Stop() is invoked
}

type StandAloneStorageReader struct {
	s     *StandAloneStorage
	txn   *badger.Txn                   // read-only txn
	iters []*engine_util.BadgerIterator // all the open iters, must be closed when Close() is called
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}

	return &StandAloneStorage{db, []*StandAloneStorageReader{}}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	for _, r := range s.readers {
		r.Close()
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{s, s.db.NewTransaction(false), []*engine_util.BadgerIterator{}}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			return engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
		case storage.Delete:
			return engine_util.DeleteCF(s.db, data.Cf, data.Key)
		}
	}
	return nil
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(r.s.db, cf, key)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if r.txn == nil {
		panic("reader shouldn't have a nil iter")
	}

	iter := engine_util.NewCFIterator(cf, r.txn)
	r.iters = append(r.iters, iter)
	return iter
}

func (r *StandAloneStorageReader) Close() {
	for _, iter := range r.iters {
		if iter.Valid() {
			iter.Close()
		}
	}

	r.txn.Discard()
}
