var sys = require('sys')
  , HDFS = require('../node-hdfs')

var hdfs = new HDFS({host:"default", port:0});

// Stat File
var statremote = function(cb) {
  var hdfs_file_path = "/tmp/test.txt"
  hdfs.stat(hdfs_file_path, function(err, data) {
    if(!err) {
      console.log("File stat of '" + data.path + "' is: " + JSON.stringify(data));
      // => {"type":"file","path":"/tmp/test","size":183,"replication":1,"block_size":33554432,"owner":"horaci","group":"wheel","permissions":420,"last_mod":1315326370,"last_access":0}
      cb();
    }
  })
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
  })
}

// Copy file to local fs (in parallel with previous read file)
var copylocal = function(cb) {
  var hdfs_file_path = "/tmp/test.txt"
  var local_out_path = "/tmp/test.out";
  hdfs.copyToLocalPath(hdfs_file_path, local_out_path, function(err, readed) {
    if(!err) {
      console.log(readed + " bytes copied from remote hdfs path " +  hdfs_file_path + " to local path " + local_out_path);
      cb();
    }
  })
}

// Write to file to HDFS
var copyremote = function(cb) {
  var hdfs_out_path = "/tmp/test_horaci_out.txt";
  var local_file_path = "/tmp/test_horaci.txt";

  var start = new Date().getTime();
  hdfs.copyFromLocalPath(local_file_path, hdfs_out_path, function(err, written) {
    if(!err) {
      var duration = new Date().getTime() - start;
      console.log(written + " bytes copied from local path " +  local_file_path + " to remote hdfs path " + hdfs_out_path + " in " + duration + " ms.");
      cb();
    }
  })
}

process.on('SIGINT', function () {
  console.log('Got SIGINT, disconnecting...');
  hdfs.disconnect();
  process.exit(0);
});

//---------------

hdfs.connect(); // optional

var ops = [statremote, readremote, copylocal, copyremote];
var nextOp = function() { var next = ops.shift(); if(next) next(nextOp);}
process.nextTick(nextOp);
