/* This code is PUBLIC DOMAIN, and is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND. See the accompanying 
 * LICENSE file.
 */

#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include "../vendor/hdfs.h"

using namespace node;
using namespace v8;

#define REQ_FUN_ARG(I, VAR)                                             \
  if (args.Length() <= (I) || !args[I]->IsFunction())                   \
    return ThrowException(Exception::TypeError(                         \
                  String::New("Argument " #I " must be a function")));  \
  Local<Function> VAR = Local<Function>::Cast(args[I]);

class HdfsClient: ObjectWrap
{
private:
  int m_count;
public:

  static Persistent<FunctionTemplate> s_ct;
  static void Init(Handle<Object> target)
  {
    HandleScope scope;

    Local<FunctionTemplate> t = FunctionTemplate::New(New);

    s_ct = Persistent<FunctionTemplate>::New(t);
    s_ct->InstanceTemplate()->SetInternalFieldCount(1);
    s_ct->SetClassName(String::NewSymbol("Hdfs"));

    NODE_SET_PROTOTYPE_METHOD(s_ct, "write", Write);

    target->Set(String::NewSymbol("Hdfs"), s_ct->GetFunction());
  }

  HdfsClient() :
    m_count(0)
  {
  }

  ~HdfsClient()
  {
  }

  static Handle<Value> New(const Arguments& args)
  {
    HandleScope scope;
    HdfsClient* hw = new HdfsClient();
    hw->Wrap(args.This());
    return args.This();
  }
  
  struct hdfs_write_baton_t {
    HdfsClient *client;
    char* filePath;
    tSize writtenBytes;
    Persistent<Function> cb;
    Persistent<Object> buffer;
  };

  static Handle<Value> Write(const Arguments& args)
  {
    HandleScope scope;
    
    REQ_FUN_ARG(2, cb);

    v8::String::Utf8Value pathStr(args[0]);
    char* writePath = (char *) malloc(strlen(*pathStr) + 1);
    strcpy(writePath, *pathStr);

    
    HdfsClient* client = ObjectWrap::Unwrap<HdfsClient>(args.This());
    
    hdfs_write_baton_t *baton = new hdfs_write_baton_t();
    baton->client = client;
    baton->cb = Persistent<Function>::New(cb);
    baton->buffer = Persistent<Object>::New(args[1]->ToObject());
    baton->writtenBytes = 0;
    baton->filePath = writePath;
    
    client->Ref();
    
    eio_custom(eio_hdfs_write, EIO_PRI_DEFAULT, eio_after_hdfs_write, baton);
    uv_ref();
    
    return Undefined();    
  }
  
  static int eio_hdfs_write(eio_req *req)
  {
    hdfs_write_baton_t *baton = static_cast<hdfs_write_baton_t*>(req->data);
    char* writePath = baton->filePath;
    
    hdfsFS fs = hdfsConnect("default", 0);
    
    char* bufData = Buffer::Data(baton->buffer);
    size_t bufLength = Buffer::Length(baton->buffer);
    
    hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY|O_CREAT, 0, 0, 0);
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)bufData, bufLength);
    
    hdfsFlush(fs, writeFile);
    hdfsCloseFile(fs, writeFile);
    
    baton->writtenBytes = num_written_bytes;

    return 0;
  }
  
  static int eio_after_hdfs_write(eio_req *req)
  {
    HandleScope scope;
    hdfs_write_baton_t *baton = static_cast<hdfs_write_baton_t*>(req->data);
    uv_unref();
    baton->client->Unref();

    Local<Value> argv[1];
    argv[0] = Integer::New(baton->writtenBytes);

    TryCatch try_catch;

    baton->cb->Call(Context::GetCurrent()->Global(), 1, argv);

    if (try_catch.HasCaught()) {
      FatalException(try_catch);
    }

    baton->cb.Dispose();

    delete baton;
    return 0;
  }
};

Persistent<FunctionTemplate> HdfsClient::s_ct;

extern "C" {
  static void init (Handle<Object> target)
  {
    HdfsClient::Init(target);
  }

  NODE_MODULE(hdfs, init);
}
