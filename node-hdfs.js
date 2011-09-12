var sys          = require('sys')
  , fs           = require('fs')
  , EventEmitter = require('events').EventEmitter
  , HDFSBindings = require('./hdfs_bindings')
  , url          = require('url')

var HDFS = new HDFSBindings.Hdfs();


var modes = {
  O_RDONLY : 0x0000,
  O_WRONLY : 0x0001,
  O_RDWR   : 0x0002,
  O_APPEND : 0x0008,
  O_CREAT  : 0x0200,
  O_TRUNC  : 0x0400
}

module.exports = function(options) {
  this.host = options.host || "default";
  this.port = options.port || 0;
  this.connected = false;

  var self = this;

  this.connect = function() {
    if(!this.connected) {
      HDFS.connect(self.host, self.port);
      this.connected = true;
    }
  }

  this.disconnect = function() {
    HDFS.disconnect();
    this.connected = false;
  }

  this.exists = function(path, cb) {
    self.connect();
    HDFS.exists(path, function(result) {
      if(result==0) {
        cb(null, true);
      } else {
        cb("file does not exist", false);
      }
    });
  }

  this.stat = function(path, cb) {
    self.connect();
    HDFS.stat(path, cb);
  }

  this.list = function(path, options, cb) {
    if (!cb && typeof options == "function") { cb = options; options = undefined; }
    self.connect();

    var pending = 0;

    HDFS.list(path,function(err, files) {
      if(!err) {
        if(!(options && options.recursive)) return cb(err, files); // not recursive
        for(i in files) {
          if(files[i].type == "directory") {
            var newPath = url.parse(files[i].path).pathname;
            pending++;
            self.list(newPath, options, function(newErr, newFiles) {
              if(newErr) {
                if(--pending == 0) cb(err, files);
              } else {
                files = files.concat(newFiles);
                if(--pending == 0) cb(err, files);
              }
            });
          }
        }
      }
      if(pending == 0) cb(err, files);
    });
  }

  this.open = function(path, mode, cb) {
    self.connect();
    HDFS.open(path, mode, cb);
  }

  this.close = function(handle, cb) {
    self.connect();
    HDFS.close(handle, cb);
  }

  this.read = function(path, bufferSize, cb) {
    if (!cb && typeof bufferSize == "function") { cb = bufferSize; bufferSize = undefined; }
    self.connect();
    var reader = new HDFSReader(path, bufferSize);
    return cb ? cb(reader) : reader;
  }

  this.write = function(path, mode, cb) {
    if (!cb && typeof mode == "function") { cb = mode; mode = undefined; }
    mode = mode || (modes.O_WRONLY | modes.O_CREAT)
    self.connect();
    var writter = new HDFSWritter(path, mode);
    return cb ? cb(writter) : writter;
  }

  this.append = function(path, cb) {
    return self.write(path, modes.O_WRONLY | modes.O_APPEND, cb)
  }

  this.mkdir = function(path,cb) {
    self.connect();
    self.exists(path, function(result) {
      if(result != 0) {
        HDFS.mkdir(path,function(result) {
          if(result == 0) {
            cb(null, true);
          } else {
            cb("Error creating directory", false);  // generic error :p
          }
        });
      } else {
        cb("File or directory already exists", false);
      }
    })
  }

  this.rm = function(path,options, cb) {
    if (!cb && typeof options == "function") { cb = options; options = undefined; }
    options = options || {recursive:false, force:false}
    self.connect();

    if(!options.force) {
      var slashes = path.split("/");
      if(slashes[0] != "") {
        return(cb("cowardly refusing to delete relative path - set force to true in options to override", false));
      }
      if( slashes.length < 3 || (slashes.length == 3 && slashes[2] == "")) {
        return(cb("cowardly refusing to delete root folder or first-level folder - set force to true in options to override", false));
      }
    }

    var delete_file = function() {
      HDFS.rm(path,function(result) {
        if(result == 0) {
          cb(null, true);
        } else {
          cb("Error deleting file", false);  // generic error :p
        }
      });
    }

    self.exists(path, function(err, result) {
      if(!err && result) {
        if(!options.recursive && !options.force) {
          self.stat(path, function(err, data) {
            if(err) {
              cb("failed stating the path or file", false);
            } else {
              if(data.type == "directory") {
                self.list(path, function(err, files) {
                  if(files && files.length > 0) {
                    cb("directory is not empty -- use recursive:true in options to force deletion", false);
                  } else { // directory empty
                    delete_file();
                  }
                });
              } else { // not a directory
                delete_file();
              }
            }
          });
        } else {  // recursive or force set
          delete_file();
        }
      } else {
        cb("File or directory does not exists", false);
      }
    })
  }

  // same as rm, but ensure path is a directory
  this.rmdir = function(path, options, cb) {
    if (!cb && typeof options == "function") { cb = options; options = undefined; }
    options = options || {}
    self.stat(path, function(err, data) {
      if(err || !data) {
        cb(err||"file not found", false);
      } else if(data.type == "directory") {
        self.rm(path, options, cb);
      } else {
        cb("not a directory", false);
      }
    })
  }

  this.copyToLocalPath = function(srcPath, dstPath, options, cb) {
    if (!cb && typeof options == "function") { cb = options; options = undefined; }
    options = options || {encoding: null, mode:0666};
    options.flags = 'w'; // force mode

    var stream = fs.createWriteStream(dstPath, options);

    stream.once('open', function(fd) {
      self.read(srcPath, function(rh) {
        var readed = 0;
        rh.on('data', function(data) {
          stream.write(data);
          readed += data.length;
        });
        rh.once('end', function(err) {
          stream.end();
          cb(err, readed);
        });
      });
    });
  }

  this.copyFromLocalPath = function(srcPath, dstPath, options, cb) {
    if (!cb || typeof cb != "function") {
      cb = options;
      options = {bufferSize: 1300, encoding: null, mode:0666, flags: 'r'};
    }

    self.write(dstPath, function(writter) {
      var written = 0;
      writter.once("open", function(handle) {
        var stream = fs.createReadStream(srcPath, options);
        stream.on("data", function(data) { writter.write(data); });
        stream.on("close", function() { writter.end();})
      });

      writter.on("write", function(len) { written += len; });
      writter.on("close", function(err) { cb(err, written); })
    })
  }
}

var HDFSReader = function(path, bufferSize) {
  var self = this;

  this.handle = null;
  this.offset = 0;
  this.length = 0;
  this.bufferSize = bufferSize || 1024*1024;

  this.read = function() {
    HDFS.read(self.handle, self.offset, self.bufferSize, function(data) {
      if(!data || data.length == 0) {
        self.end();
      } else {
        self.emit("data", data);
        self.offset += data.length;
        data.length < self.bufferSize ? self.end() : self.read();
      }
    });
  };

  this.end = function(err) {
    if(self.handle) {
      HDFS.close(self.handle, function() {
        self.emit("end", err);
      })
    } else {
      self.emit("end", err);
    }
  }

  HDFS.open(path, modes.O_RDONLY, function(err, handle) {
    if(err) {
      self.end(err);
    } else {
      self.emit("open", handle);
      self.handle = handle;
      self.read();
    }
  });

  EventEmitter.call(this);
}

sys.inherits(HDFSReader, EventEmitter);

var HDFSWritter = function(path, mode) {
  var self = this;
  this.handle = null;
  this.writting = false;
  this.closeCalled = false;
  this.writeBuffer = new Buffer(0);
  mode = mode || (modes.O_WRONLY | modes.O_CREAT)

  this.write = function(buffer) {
    self.expandBuffer(buffer);
    if(self.handle >= 0) {
      if(!self.writting) {
        self.writting = true;
        var newBuffer = self.writeBuffer;
        self.writeBuffer = new Buffer(0);
        HDFS.write(self.handle, newBuffer, function(len) {
          self.writting = false
          self.emit("write", len);
          if(self.closeCalled) {
            self.writeBuffer.length > 0 ? self.write() : self.end();
          }
        });
      }
    }
  };

  this.end = function(err) {
    if(self.handle >= 0) {
      if(!self.writting && self.writeBuffer.length == 0) {
        HDFS.close(self.handle, function() {
          self.handle = undefined;
          self.emit("close", err);
        })
      } else {
        self.closeCalled = true;
        self.write();
      }
    } else {
      self.emit("close", err);
    }
  }

  this.expandBuffer = function(buffer) {
    if(buffer) {
      if(buffer.constructor.name != "Buffer") {
        buffer = new Buffer(buffer.toString());
      }
      var newBuffer = new Buffer(self.writeBuffer.length + buffer.length);
      self.writeBuffer.copy(newBuffer, 0, 0);
      buffer.copy(newBuffer, self.writeBuffer.length, 0);
      self.writeBuffer = newBuffer;
    }
  }
  
  HDFS.open(path, mode, function(err, handle) {
    if(err || !(handle >= 0)) {
      self.end(err);
    } else {
      self.handle = handle;
      self.emit("open", err, handle);
    }
  });

  EventEmitter.call(this);
}

sys.inherits(HDFSWritter, EventEmitter);
