package adapter

import (
	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/thanos-io/objstore"
)

// NewObjstoreAdapter is a deprecated compatibility shim for pkg/store/objectstore.New.
func NewObjstoreAdapter(bucket objstore.Bucket, logger log.Logger, opts ...objectstore.Option) daramjwee.Store {
	return objectstore.New(bucket, logger, opts...)
}
