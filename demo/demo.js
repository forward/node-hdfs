var h = require('../build/default/hdfs.node');
var hi = new h.Hdfs();
var data = new Buffer("Hello, my name is Paul", encoding='utf8')
var writtenBytes = hi.write("/tmp/testfile.txt", data, function(bytes) {
  console.log("Wrote file with " + bytes + ' bytes.');
});
console.log("Finished");
// console.log("Wrote " + writtenBytes + " bytes")
