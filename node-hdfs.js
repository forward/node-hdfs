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

  this.write = function(path, cb) {
    self.connect();
    var writter = new HDFSWritter(path);
    return cb ? cb(writter) : writter;
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

var HDFSWritter = function(path) {
  var self = this;
  this.handle = null;
  this.writting = false;
  this.closeCalled = false;
  this.writeBuffer = new Buffer(0);

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
      var newBuffer = new Buffer(self.writeBuffer.length + buffer.length);
      self.writeBuffer.copy(newBuffer, 0, 0);
      buffer.copy(newBuffer, self.writeBuffer.length, 0);
      self.writeBuffer = newBuffer;
    }
  }

  HDFS.open(path, modes.O_WRONLY | modes.O_CREAT, function(err, handle) {
    if(err) {
      self.end(err);
    } else {
      self.handle = handle;
      self.emit("open", err, handle);
    }
  });

  EventEmitter.call(this);
}

sys.inherits(HDFSWritter, EventEmitter);
