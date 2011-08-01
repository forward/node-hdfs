var h = require('../build/default/hdfs.node');
var hi = new h.Hdfs();
var data = new Buffer("Hello, my name is Paul", encoding='utf8')

var writtenBytes = hi.write("/tmp/testfile.txt", data, function(bytes) {
  console.log("Wrote file with " + bytes + " bytes.\n");
  console.log("About to start reading byes...")
  
  hi.read("/tmp/testfile.txt", function(data) {
    console.log("Data was: " + data);
  });
});
console.log("Finished outer write\n");

// console.log("Wrote " + writtenBytes + " bytes")


// hi.openForWriting("/tmp/tetfile.txt", function(f) {
//   f.write(buffer, function(bytes) {
//     console.log("I just wrote some bytes");
//   })
// })
// 
