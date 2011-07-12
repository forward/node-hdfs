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

  static Handle<Value> Write(const Arguments& args)
  {
    HandleScope scope;
    
    hdfsFS fs = hdfsConnect("default", 0);
    v8::String::Utf8Value pathStr(args[0]);
    char* writePath = (char *) malloc(strlen(*pathStr) + 1);
    strcpy(writePath, *pathStr);
    
    char* bufData = Buffer::Data(args[1]->ToObject());
    size_t bufLength = Buffer::Length(args[1]->ToObject());
    
    hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY|O_CREAT, 0, 0, 0);
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)bufData, bufLength);
    hdfsFlush(fs, writeFile);
    hdfsCloseFile(fs, writeFile);
    
    HdfsClient* hw = ObjectWrap::Unwrap<HdfsClient>(args.This());
    hw->m_count++;
    Local<Integer> result = Integer::New(num_written_bytes);

    return scope.Close(result);
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
