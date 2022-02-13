package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	r := kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	r.Value, _ = reader.GetCF(req.Cf, req.Key)
	if r.Value == nil {
		r.NotFound = true
	}
	return &r, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	r := kvrpcpb.RawPutResponse{}

	m := []storage.Modify{
		{
			Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf},
		},
	}
	err := server.storage.Write(req.Context, m)

	return &r, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	r := kvrpcpb.RawDeleteResponse{}

	m := []storage.Modify{
		{
			Data: storage.Delete{Key: req.Key, Cf: req.Cf},
		},
	}
	err := server.storage.Write(req.Context, m)

	return &r, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r := kvrpcpb.RawScanResponse{}
	r.Kvs = []*kvrpcpb.KvPair{}

	reader, err := server.storage.Reader(req.Context)
	iter := reader.IterCF(req.Cf)

	iter.Seek(req.StartKey) // start from the StartKey
	for itcount := 0; itcount < int(req.Limit) && iter.Valid(); itcount++ {
		k := iter.Item().Key()
		v, _ := iter.Item().Value()
		r.Kvs = append(r.Kvs, &kvrpcpb.KvPair{Key: k, Value: v})
		iter.Next()
	}

	// close the iter
	iter.Close()

	return &r, err
}
