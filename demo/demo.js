var sys = require('sys')
  , HDFS = require('../node-hdfs');

var hdfs = new HDFS({host:"default", port:0});

// create file (overwrite if exists)
var writeremote = function(cb) {
  var hdfs_path = "/tmp/test.txt"
  var buffer = new Buffer("The path of the righteous man is beset on all sides by the iniquities of the\n" +
                          "selfish and the tyranny of evil men. Blessed is he who in the name of\n" + 
                          "charity and good will shepherds the weak through the valley of darkness, for\n" +
                          "he is truly his brother's keeper and the finder of lost children. And I will\n" +
                          "strike down upon thee with great vengeance and furious anger those who\n" +
                          "attempt to poison and destroy my brothers. And you will know my name is the\n" +
                          "Lord when I lay my vengeance upon thee.\n")

  hdfs.write(hdfs_path, function(writter) {
    writter.once("open", function(err, handle) {
      writter.write(buffer);
      writter.end();
    })
    writter.once("close", function(err) {
      cb();
    });
  });
}

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
      }
      cb();
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
    }
    cb();
  });
}

// Write local file to HDFS path
var copyremote = function(cb) {
  var local_file_path = "/tmp/test_horaci.txt";
  var hdfs_out_path = "/tmp/test_horaci.txt";

  var start = new Date().getTime();
  hdfs.copyFromLocalPath(local_file_path, hdfs_out_path, function(err, written) {
    if(!err) {
      var duration = new Date().getTime() - start;
      console.log(written + " bytes copied from local path " +  local_file_path + " to remote hdfs path " + hdfs_out_path + " in " + duration + " ms.");
    }
    cb();
  });
}

var createdirectory = function(cb) {
  var hdfs_path = "/tmp/node-hdfs-test/a/b/c/d/e";
  hdfs.mkdir(hdfs_path, function(err, result) {
    if(!err && result) {
      console.log("Directory " + hdfs_path + " created.");
    } else {
      console.log("Failed creating directory " + hdfs_path + ": " + err);
    }
    cb();
  });
}

var deletedirectory = function(cb) {
  var hdfs_path = "/tmp/node-hdfs-test/a/b/c/d/e";
  hdfs.rm(hdfs_path, {}, function(err, result) {
    if(!err && result) {
      console.log("Directory " + hdfs_path + " deleted.");
    } else {
      console.log("Failed deleting " + hdfs_path + ": " + err);
    }

    var hdfs_path_non_recursive = "/tmp/node-hdfs-test/a/b/c";
    hdfs.rm(hdfs_path_non_recursive, {}, function(err, result) {
      if(!err && result) {
        console.log("Glups!, deleted a folder " + hdfs_path_non_recursive + " with subfolders without recursive or force flags?");
      } else {
        console.log("Correctly refused deleting " + hdfs_path_non_recursive + ": " + err);
      }
      
      var hdfs_path_recursive = "/tmp/node-hdfs-test/a/b/c";
      hdfs.rm(hdfs_path_recursive, {recursive:true}, function(err, result) {
        if(!err && result) {
          console.log("Deleted folder " + hdfs_path_recursive + " with recursive flag");
        } else {
          console.log("Error deleting folder " + hdfs_path_recursive + ": " + err);
        }
      
        var hdfs_path_force = "/tmp/node-hdfs-test/a";
        hdfs.rm(hdfs_path_force, {force:true}, function(err, result) {
          if(!err && result) {
            console.log("Deleted folder " + hdfs_path_force + " with force flag");
          } else {
            console.log("Error deleting folder " + hdfs_path_force + ": " + err);
          }

          var hdfs_path_dir = "/tmp/node-hdfs-test/";
          hdfs.rmdir(hdfs_path_dir, function(err, result) {
            if(!err && result) {
              console.log("Removed empty directory " + hdfs_path_dir)
            } else {
              console.log("Failed removing empty directory " + hdfs_path_dir)
            }
            
            var hdfs_path_file = "/tmp/test.txt";
            hdfs.rmdir(hdfs_path_file, function(err, result) {
              if(!err && result) {
                console.log("Glups! Removed file with rmdir: " + hdfs_path_file)
              } else {
                console.log("Correctly refused deleting file with rmdir " + hdfs_path_file + ": " + err)
              }
              cb();
            })
          })
        });
      });
    });
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

var appendfile = function(cb) {
  var hdfs_path = "/tmp/test-horaci2.txt"  

  var append_file = function(str, callback) {
    hdfs.append(hdfs_path, function(appender) {
      appender.on("open", function( err, handle) {
        appender.write(new Buffer(str));
        appender.end();
      })
      appender.on("close", function(err) {
        callback();
      });
    })
  }
  
  var create_file = function(callback) {
    hdfs.write(hdfs_path, function(writter) {
      writter.once("open", function(err, handle) {
        writter.write(new Buffer("Hello create file.\n"));
        writter.end();
      })
      writter.on("close", function(err) {
        callback();
      });
    });
  }

  create_file(function() { 
    console.log("Create file with some data: " + hdfs_path);
    append_file("First append\n", function() {
      append_file("Second append\n", function() {
        console.log("Appended data to file: " + hdfs_path);
        cb();
      })
    })
  });
}

//---------------

console.log("Connecting to HDFS server...");
hdfs.connect(); // (optional, first command will connect if disconnected)
console.log("Connected!");

var ops = [writeremote, statremote, readremote, copylocal, copyremote, listremote, listremoterecursive, createdirectory, deletedirectory, appendfile];
var nextOp = function() { if(next=ops.shift()) next(nextOp)}
process.nextTick(nextOp);

//--------------------

process.on('SIGINT', function () {
  console.log('Got SIGINT, disconnecting...');
  hdfs.disconnect();
  process.exit(0);
});