/* This code is PUBLIC DOMAIN, and is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND. See the accompanying
 * LICENSE file.
 */

#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include <node_object_wrap.h>
#include <unistd.h>
#include "../vendor/hdfs.h"

using namespace node;
using namespace v8;

#define REQ_FUN_ARG(I, VAR)                                             \
  if (args.Length() <= (I) || !args[I]->IsFunction())                   \
    return ThrowException(Exception::TypeError(                         \
                  String::New("Argument " #I " must be a function")));  \
  Local<Function> VAR = Local<Function>::Cast(args[I]);

class HdfsClient : public ObjectWrap
{
private:
  int m_count;
  hdfsFS fs_;

  int fh_count_;
  hdfsFile_internal **fh_;
public:

  static Persistent<FunctionTemplate> s_ct;
  static void Init(Handle<Object> target)
  {
    HandleScope scope;

    Local<FunctionTemplate> t = FunctionTemplate::New(New);

    s_ct = Persistent<FunctionTemplate>::New(t);
    s_ct->InstanceTemplate()->SetInternalFieldCount(1);
    s_ct->SetClassName(String::NewSymbol("HdfsBindings"));

    NODE_SET_PROTOTYPE_METHOD(s_ct, "connect", Connect);
    NODE_SET_PROTOTYPE_METHOD(s_ct, "write", Write);
    NODE_SET_PROTOTYPE_METHOD(s_ct, "read", Read);
    NODE_SET_PROTOTYPE_METHOD(s_ct, "stat", Stat);
    NODE_SET_PROTOTYPE_METHOD(s_ct, "open", Open);
    NODE_SET_PROTOTYPE_METHOD(s_ct, "close", Close);
    NODE_SET_PROTOTYPE_METHOD(s_ct, "disconnect", Disconnect);

    target->Set(String::NewSymbol("Hdfs"), s_ct->GetFunction());
  }

  HdfsClient()
  {
    m_count = 0;
    fs_ = NULL;
    fh_count_ = 1024;
    fh_ = (hdfsFile_internal **) calloc(fh_count_, sizeof(hdfsFile_internal *));
    memset(fh_, 0, sizeof(fh_count_ * sizeof(hdfsFile_internal **)));
  }

  ~HdfsClient()
  {
    free(fh_);
  }

  static Handle<Value> New(const Arguments& args)
  {
    HandleScope scope;
    HdfsClient* client = new HdfsClient();
    client->Wrap(args.This());
    return args.This();
  }

  struct hdfs_open_baton_t {
    HdfsClient *client;
    char *filePath;
    Persistent<Function> cb;
    hdfsFile_internal *fileHandle;
    int flags;
  };

  struct hdfs_write_baton_t {
    HdfsClient *client;
    char *buffer;
    int bufferLength;
    hdfsFile_internal *fileHandle;
    Persistent<Function> cb;
    tSize writtenBytes;
  };

  struct hdfs_read_baton_t {
    HdfsClient *client;
    int fh;
    int bufferSize;
    int offset;
    hdfsFile_internal *fileHandle;
    Persistent<Function> cb;
    char *buffer;
    int readBytes;
  };

  struct hdfs_stat_baton_t {
    HdfsClient *client;
    char *filePath;
    hdfsFileInfo *fileStat;
    Persistent<Function> cb;
  };

  struct hdfs_close_baton_t {
    HdfsClient *client;
    int fh;
    hdfsFile_internal *fileHandle;
    Persistent<Function> cb;
  };


  static Handle<Value> Connect(const Arguments &args)
  {
    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());
    v8::String::Utf8Value hostStr(args[0]);
    client->fs_ = hdfsConnectNewInstance(*hostStr, args[1]->Int32Value());
    return Boolean::New(client->fs_ ? true : false);
  }

  static Handle<Value> Disconnect(const Arguments &args)
  {
    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());
    hdfsDisconnect(client->fs_);
    return Boolean::New(true);
  }

  static Handle<Value> Stat(const Arguments &args)
  {
    HandleScope scope;

    REQ_FUN_ARG(1, cb);

    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());

    v8::String::Utf8Value pathStr(args[0]);
    char* statPath = new char[strlen(*pathStr) + 1];
    strcpy(statPath, *pathStr);

    hdfs_stat_baton_t *baton = new hdfs_stat_baton_t();
    baton->client = client;
    baton->cb = Persistent<Function>::New(cb);
    baton->filePath = statPath;
    baton->fileStat = NULL;

    client->Ref();

    eio_custom(eio_hdfs_stat, EIO_PRI_DEFAULT, eio_after_hdfs_stat, baton);
    ev_ref(EV_DEFAULT_UC);

    return Undefined();
  }

  static int eio_hdfs_stat(eio_req *req)
  {
    hdfs_stat_baton_t *baton = static_cast<hdfs_stat_baton_t*>(req->data);
    baton->fileStat = hdfsGetPathInfo(baton->client->fs_, baton->filePath);
    return 0;
  }

  static int eio_after_hdfs_stat(eio_req *req)
  {
    HandleScope scope;
    hdfs_stat_baton_t *baton = static_cast<hdfs_stat_baton_t*>(req->data);
    ev_unref(EV_DEFAULT_UC);
    baton->client->Unref();

    Handle<Value> argv[2];

    if(baton->fileStat) {
      Persistent<Object> statObject = Persistent<Object>::New(Object::New());;

      char *path = baton->fileStat->mName;

      char kind = (char)baton->fileStat->mKind;

      statObject->Set(String::New("type"),        String::New(kind == 'F' ? "file" : kind == 'D' ? "directory" : "other"));
      statObject->Set(String::New("path"),        String::New(path));
      statObject->Set(String::New("size"),        Integer::New(baton->fileStat->mSize));
      statObject->Set(String::New("replication"), Integer::New(baton->fileStat->mReplication));
      statObject->Set(String::New("block_size"),  Integer::New(baton->fileStat->mBlockSize));
      statObject->Set(String::New("owner"),       String::New(baton->fileStat->mOwner));
      statObject->Set(String::New("group"),       String::New(baton->fileStat->mGroup));
      statObject->Set(String::New("permissions"), Integer::New(baton->fileStat->mPermissions));
      statObject->Set(String::New("last_mod"),    Integer::New(baton->fileStat->mLastMod));
      statObject->Set(String::New("last_access"), Integer::New(baton->fileStat->mLastAccess));

      argv[0] = Local<Value>::New(Undefined());
      argv[1] = Local<Value>::New(statObject);

      hdfsFreeFileInfo(baton->fileStat, 1);
    } else {
      argv[0] = Local<Value>::New(String::New("File does not exist"));
      argv[1] = Local<Value>::New(Undefined());
    }

    TryCatch try_catch;
    baton->cb->Call(Context::GetCurrent()->Global(), 2, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    baton->cb.Dispose();

    delete baton;
    return 0;
  }


  /**********************/
  /* Open               */
  /**********************/
  // open(char *path, int flags, callback)

  static Handle<Value> Open(const Arguments &args)
  {
    HandleScope scope;
    REQ_FUN_ARG(2, cb);

    // get client
    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());

    // Parse path
    v8::String::Utf8Value pathStr(args[0]);
    char* statPath = new char[strlen(*pathStr) + 1];
    strcpy(statPath, *pathStr);

    // Initialize baton
    hdfs_open_baton_t *baton = new hdfs_open_baton_t();
    baton->client = client;
    baton->cb = Persistent<Function>::New(cb);
    baton->filePath = statPath;
    baton->fileHandle = NULL;
    baton->flags = args[1]->Int32Value();

    client->Ref();

    // Call eio operation
    eio_custom(eio_hdfs_open, EIO_PRI_DEFAULT, eio_after_hdfs_open, baton);
    ev_ref(EV_DEFAULT_UC);

    return Undefined();
  }

  static int eio_hdfs_open(eio_req *req)
  {
    hdfs_open_baton_t *baton = static_cast<hdfs_open_baton_t*>(req->data);
    baton->fileHandle = hdfsOpenFile(baton->client->fs_, baton->filePath, baton->flags, 0, 0, 0);
    return 0;
  }

  int CreateFileHandle(hdfsFile_internal *f)
  {
    // TODO: quick and dirty, totally inefficient!
    int fh = 0;
    while(fh < fh_count_ && fh_[fh]) fh++;
    if(fh >= fh_count_) return -1;
    fh_[fh] = f;
    return fh;
  }

  hdfsFile_internal *GetFileHandle(int fh)
  {
    return fh_[fh];
  }

  void RemoveFileHandle(int fh)
  {
    fh_[fh] = NULL;
  }

  static int eio_after_hdfs_open(eio_req *req)
  {
    HandleScope scope;
    hdfs_open_baton_t *baton = static_cast<hdfs_open_baton_t*>(req->data);

    ev_unref(EV_DEFAULT_UC);
    baton->client->Unref();

    Handle<Value> argv[2];

    if(baton->fileHandle) {
      int fh = baton->client->CreateFileHandle(baton->fileHandle);
      if(fh >= 0) {
        argv[0] = Local<Value>::New(Undefined());
        argv[1] = Local<Value>::New(Integer::New(fh));
      } else {
        argv[0] = Local<Value>::New(String::New("Too many open files"));
        argv[1] = Local<Value>::New(Undefined());
      }
    } else {
      argv[0] = Local<Value>::New(String::New("File does not exist"));
      argv[1] = Local<Value>::New(Undefined());
    }

    TryCatch try_catch;
    baton->cb->Call(Context::GetCurrent()->Global(), 2, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    baton->cb.Dispose();

    delete baton;
    return 0;
  }

  /**********************/
  /* Close               */
  /**********************/

  static Handle<Value> Close(const Arguments &args)
  {
    HandleScope scope;
    REQ_FUN_ARG(1, cb);

    // get client
    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());

    // Initialize baton
    hdfs_close_baton_t *baton = new hdfs_close_baton_t();
    baton->client = client;
    baton->cb = Persistent<Function>::New(cb);
    baton->fh = args[0]->Int32Value();
    baton->fileHandle = client->GetFileHandle(baton->fh);

    client->Ref();

    // Call eio operation
    eio_custom(eio_hdfs_close, EIO_PRI_DEFAULT, eio_after_hdfs_close, baton);
    ev_ref(EV_DEFAULT_UC);

    return Undefined();
  }

  static int eio_hdfs_close(eio_req *req)
  {
    hdfs_close_baton_t *baton = static_cast<hdfs_close_baton_t*>(req->data);
    if(baton->fileHandle) hdfsCloseFile(baton->client->fs_, baton->fileHandle);
    return 0;
  }

  static int eio_after_hdfs_close(eio_req *req)
  {
    HandleScope scope;
    hdfs_close_baton_t *baton = static_cast<hdfs_close_baton_t*>(req->data);

    ev_unref(EV_DEFAULT_UC);
    baton->client->Unref();
    baton->client->RemoveFileHandle(baton->fh);

    TryCatch try_catch;
    baton->cb->Call(Context::GetCurrent()->Global(), 0, NULL);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    baton->cb.Dispose();

    delete baton;
    return 0;
  }

  /**********************/
  /* READ               */
  /**********************/

  // handle, offset, bufferSize, callback
  static Handle<Value> Read(const Arguments &args)
  {
    HandleScope scope;
    REQ_FUN_ARG(3, cb);

    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());

    int fh = args[0]->Int32Value();

    hdfsFile_internal *fileHandle = client->GetFileHandle(fh);

    if(!fileHandle) {
      return ThrowException(Exception::TypeError(String::New("Invalid file handle")));
    }

    hdfs_read_baton_t *baton = new hdfs_read_baton_t();
    baton->client = client;
    baton->cb = Persistent<Function>::New(cb);
    baton->fileHandle = fileHandle;
    baton->offset = args[1]->Int32Value();
    baton->bufferSize = args[2]->Int32Value();
    baton->fh = fh;

    client->Ref();

    eio_custom(eio_hdfs_read, EIO_PRI_DEFAULT, eio_after_hdfs_read, baton);
    ev_ref(EV_DEFAULT_UC);

    return Undefined();
  }

  static int eio_hdfs_read(eio_req *req)
  {
    hdfs_read_baton_t *baton = static_cast<hdfs_read_baton_t*>(req->data);
    baton->buffer = (char *) malloc(baton->bufferSize * sizeof(char));
    baton->readBytes = hdfsPread(baton->client->fs_, baton->fileHandle, baton->offset, baton->buffer, baton->bufferSize);
    return 0;
  }

  static int eio_after_hdfs_read(eio_req *req)
  {
    HandleScope scope;

    hdfs_read_baton_t *baton = static_cast<hdfs_read_baton_t*>(req->data);
    ev_unref(EV_DEFAULT_UC);
    baton->client->Unref();

    Handle<Value> argv[1];

    Buffer *b =  Buffer::New(baton->buffer, baton->readBytes);
    argv[0] = Local<Value>::New(b->handle_);
    free(baton->buffer);

    TryCatch try_catch;
    baton->cb->Call(Context::GetCurrent()->Global(), 1, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    baton->cb.Dispose();
    delete baton;
    return 0;
  }

  /**********************/
  /* WRITE              */
  /**********************/

  // write(fileHandleId, buffer, cb)
  static Handle<Value> Write(const Arguments& args)
  {


    HandleScope scope;
    REQ_FUN_ARG(2, cb);

    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());
    int fh = args[0]->Int32Value();
    hdfsFile_internal *fileHandle = client->GetFileHandle(fh);

    if(!fileHandle) {
      return ThrowException(Exception::TypeError(String::New("Invalid file handle")));
    }

    Local<Object> obj = args[1]->ToObject();
    int length = Buffer::Length(obj);
    char *buffer = (char *) malloc(length * sizeof(char));
    strncpy(buffer, Buffer::Data(obj), length);

    hdfs_write_baton_t *baton = new hdfs_write_baton_t();
    baton->client = client;
    baton->cb = Persistent<Function>::New(cb);
    baton->buffer = buffer;
    baton->bufferLength = length;
    baton->fileHandle = fileHandle;
    baton->writtenBytes = 0;

    client->Ref();

    eio_custom(eio_hdfs_write, EIO_PRI_DEFAULT, eio_after_hdfs_write, baton);
    ev_ref(EV_DEFAULT_UC);

    return Undefined();
  }

  static int eio_hdfs_write(eio_req *req)
  {
    hdfs_write_baton_t *baton = static_cast<hdfs_write_baton_t*>(req->data);

    baton->writtenBytes = hdfsWrite(baton->client->fs_, baton->fileHandle, (void*)baton->buffer, baton->bufferLength);
    hdfsFlush(baton->client->fs_, baton->fileHandle);

    return 0;
  }

  static int eio_after_hdfs_write(eio_req *req)
  {
    HandleScope scope;
    hdfs_write_baton_t *baton = static_cast<hdfs_write_baton_t*>(req->data);

    ev_unref(EV_DEFAULT_UC);
    baton->client->Unref();

    Local<Value> argv[1];
    argv[0] = Integer::New(baton->writtenBytes);

    TryCatch try_catch;

    baton->cb->Call(Context::GetCurrent()->Global(), 1, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    baton->cb.Dispose();

    free(baton->buffer);
    delete baton;
    return 0;
  }
};

Persistent<FunctionTemplate> HdfsClient::s_ct;

extern "C" {
  static void init (Handle<Object> target)
  {
    v8::ResourceConstraints rc;
    rc.set_stack_limit((uint32_t *)1);
    v8::SetResourceConstraints(&rc);

    HdfsClient::Init(target);
  }

  NODE_MODULE(hdfs_bindings, init);
}
