var sys = require('sys')
  , HDFS = require('../node-hdfs');

var hdfs = new HDFS({host:"default", port:0});

// Stat File
var statremote = function(cb) {
  var hdfs_file_path = "/tmp/test.txt"
  hdfs.stat(hdfs_file_path, function(err, data) {
    if(!err) {
      console.log("File stat of '" + data.path + "' is: " + JSON.stringify(data));
      // => {"type":"file","path":"hdfs://master:9000/tmp/test.txt","size":487,"replication":1,"block_size":67108864,"owner":"horaci","group":"supergroup","permissions":420,"last_mod":1315563055,"last_access":1315563055}
      cb();
    }
  });
}

// Read file
var readremote = function(cb) {
  var hdfs_file_path = "/tmp/test.txt"
  hdfs.read(hdfs_file_path, 1024*1024, function(reader) {
    var readed = 0;
    reader.on("open", function(handle) {
      // do something when open
    });
    reader.on("data", function(data) {
      readed += data.length;
    });
    reader.on("end", function(err) {
      if(!err) {
        console.log("Finished reading data - Total readed: " + readed);
        cb();
      }
    });
  });
}

// Copy HDFS file to local path
var copylocal = function(cb) {
  var hdfs_file_path = "/tmp/test.txt"
  var local_out_path = "/tmp/test_horaci.txt";
  hdfs.copyToLocalPath(hdfs_file_path, local_out_path, function(err, readed) {
    if(!err) {
      console.log(readed + " bytes copied from remote hdfs path " +  hdfs_file_path + " to local path " + local_out_path);
      cb();
    }
  });
}

// Write local file to HDFS path
var copyremote = function(cb) {
  var local_file_path = "/tmp/test_horaci.txt";
  var hdfs_out_path = "/tmp/test.txt";

  var start = new Date().getTime();
  hdfs.copyFromLocalPath(local_file_path, hdfs_out_path, function(err, written) {
    if(!err) {
      var duration = new Date().getTime() - start;
      console.log(written + " bytes copied from local path " +  local_file_path + " to remote hdfs path " + hdfs_out_path + " in " + duration + " ms.");
      cb();
    }
  });
}

var listremote = function(cb) {
  var hdfs_path = "/user/horaci"
  hdfs.list(hdfs_path, function(err, files) {
    if(!err) {
      console.log("Listing non-recursive of " + hdfs_path + " has " + files.length + " files");
    }
    cb();
  });
}


var listremoterecursive = function(cb) {
  var hdfs_path = "/user/horaci"
  hdfs.list(hdfs_path, {recursive:true}, function(err, files) {
    if(!err) {
      console.log("Listing recursive of " + hdfs_path + " has " + files.length + " files");
    }
    cb();
  });
}

//---------------

console.log("Connecting to HDFS server...");
hdfs.connect(); // (optional, first command will connect if disconnected)
console.log("Connected!");

var ops = [statremote, readremote, copylocal, copyremote, listremote, listremoterecursive];
//var ops = [listremote,listremoterecursive];
var nextOp = function() { if(next=ops.shift()) next(nextOp)}
process.nextTick(nextOp);

//--------------------

process.on('SIGINT', function () {
  console.log('Got SIGINT, disconnecting...');
  hdfs.disconnect();
  process.exit(0);
});