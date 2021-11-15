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
	ret := &kvrpcpb.RawGetResponse{NotFound: false}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		ret.NotFound = true
	}
	ret.Value = val
	return ret, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	if err := server.storage.Write(req.Context, []storage.Modify{{Data: put}}); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	put := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	if err := server.storage.Write(req.Context, []storage.Modify{{Data: put}}); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	ret := &kvrpcpb.RawScanResponse{Kvs: make([]*kvrpcpb.KvPair, 0)}
	for iter.Seek(req.StartKey); limit > 0 && iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		pair := &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: val,
		}
		ret.Kvs = append(ret.Kvs, pair)
		limit--
	}
	return ret, nil
}
