var sys = require('sys')
  , HDFS = require('../node-hdfs')

var hdfs = new HDFS({host:"default", port:0});

var hdfs_file_path = "/tmp/test.txt"
var local_out_path = "/tmp/test.out";

// Stat File
hdfs.stat(hdfs_file_path, function(err, data) {
  if(!err) {
    console.log("File stat of '" + data.path + "' is: " + JSON.stringify(data)); 
    // => {"type":"file","path":"/tmp/test","size":183,"replication":1,"block_size":33554432,"owner":"horaci","group":"wheel","permissions":420,"last_mod":1315326370,"last_access":0}
  }
})

// Read file
hdfs.read(hdfs_file_path, 1024*1024, function(reader) {
  var readed = 0;
  reader.on("open", function(handle) {
    console.log("File " + hdfs_file_path + " opened.")
  });
  reader.on("data", function(data) {
    readed += data.length;
    console.log("readed " + data.length + " bytes (" + readed +")");
  });
  reader.on("end", function(err) {
    if(!err) {
      console.log("Finished reading data - Total readed: " + readed);
    }
  });
})

// Copy file to local fs (in parallel with previous read file)
hdfs.copyToLocalPath(hdfs_file_path, local_out_path, function(err, readed) {
  if(!err) {
    console.log(readed + " bytes copied from " +  hdfs_file_path + " to " + local_out_path);
  }
})
