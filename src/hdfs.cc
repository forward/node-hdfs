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

class HelloWorld: ObjectWrap
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

    NODE_SET_PROTOTYPE_METHOD(s_ct, "hello", Hello);

    target->Set(String::NewSymbol("Hdfs"), s_ct->GetFunction());
  }

  HelloWorld() :
    m_count(0)
  {
  }

  ~HelloWorld()
  {
  }

  static Handle<Value> New(const Arguments& args)
  {
    HandleScope scope;
    HelloWorld* hw = new HelloWorld();
    hw->Wrap(args.This());
    return args.This();
  }

  static Handle<Value> Hello(const Arguments& args)
  {
    HandleScope scope;
    
    hdfsFS fs = hdfsConnect("default", 0);
    v8::String::Utf8Value pathStr(args[0]);
    char* writePath = (char *) malloc(strlen(*pathStr) + 1);
    strcpy(writePath, *pathStr);
    
    // Handle<Buffer> bufArg = Buffer::Value(args[1]);
    char* bufData = Buffer::Data(args[1]->ToObject());
    size_t bufLength = Buffer::Length(args[1]->ToObject());
    
    // v8::String::Utf8Value bufferStr(args[1]);
    // char* buffer = (char *) malloc(strlen(*bufferStr) + 1);
    // strcpy(buffer, *bufferStr);
    
    // const char* writePath = "/tmp/testfile.txt";
    hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY|O_CREAT, 0, 0, 0);
    // char* buffer = "Hello, World!";
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)bufData, bufLength);
    hdfsFlush(fs, writeFile);
    hdfsCloseFile(fs, writeFile);
    
    HelloWorld* hw = ObjectWrap::Unwrap<HelloWorld>(args.This());
    hw->m_count++;
    Local<String> result = String::New("Hello World");

    return scope.Close(result);
  }

};

Persistent<FunctionTemplate> HelloWorld::s_ct;

extern "C" {
  static void init (Handle<Object> target)
  {
    HelloWorld::Init(target);
  }

  NODE_MODULE(hdfs, init);
}
