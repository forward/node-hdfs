var sys          = require('sys')
  , fs           = require('fs')
  , EventEmitter = require('events').EventEmitter
  , HDFSBindings = require('./hdfs_bindings')

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

  this.stat = function(path, cb) {
    self.connect();
    HDFS.stat(path, cb);
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
    if (!cb || typeof cb != "function") { 
      cb = bufferSize; 
      bufferSize = 1024*1024;
    }

    self.connect();
    var reader = new HDFSReader(path, bufferSize);
    if(cb) {
      cb(reader);
    } else {
      return reader;
    }
  }
  
  this.copyToLocalPath = function(srcPath, dstPath, options, cb) {
    if (!cb || typeof cb != "function") { 
      cb = options; 
      options = {encoding: null, mode:0666, flags: 'w'};
    }
    var stream = fs.createWriteStream(dstPath, options);
    var readed = 0;
    var bufferSize = options.bufferSize || 1024*1024; // 1mb chunks by default

    stream.once('open', function(fd) {
      self.read(srcPath, bufferSize, function(rh) { 
        rh.on('data', function(data) {
          stream.write(data);
          readed += data.length;
        });
        rh.once('end', function(err) {
          stream.end();
          cb(err, readed);
        });
      })
    });
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
