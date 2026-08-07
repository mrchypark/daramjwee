package daramjwee

// Note: safeCloser and cancelOnCloseReadCloser are NOT pooled because
// their Close() methods return errors that callers read after the call,
// creating a use-after-free race if the object is returned to pool inside Do().
//
// staleRefreshCallback IS pooled because its handle() method has no return value.
// byteReadCloser IS pooled because its Close() return value is always nil.
