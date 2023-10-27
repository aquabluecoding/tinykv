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
	// Your Code Here (1).
	r, _ := server.storage.Reader(nil)
	defer r.Close()
	rr, err := r.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if rr == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: rr}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	p := storage.Put{req.Key, req.Value, req.Cf}
	m := storage.Modify{p}
	err := server.storage.Write(nil, []storage.Modify{m})
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	d := storage.Delete{req.Key, req.Cf}
	m := storage.Modify{d}
	err := server.storage.Write(nil, []storage.Modify{m})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, _ := server.storage.Reader(nil)
	it := r.IterCF(req.Cf)
	defer r.Close()
	defer it.Close()
	limit := req.Limit
	kvs := []*kvrpcpb.KvPair{}
	for it.Seek(req.StartKey); it.Valid() && limit > 0; it.Next() {
		item := it.Item()
		value, _ := item.ValueCopy(nil)
		kv := &kvrpcpb.KvPair{Error: nil, Key: item.KeyCopy(nil), Value: value}
		kvs = append(kvs, kv)
		limit--
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
