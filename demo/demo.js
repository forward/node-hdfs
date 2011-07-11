var h = require('../build/default/hdfs.node');
var hi = new h.Hdfs();
var data = new Buffer("Hello, my name is Paul", encoding='utf8')
hi.hello("/tmp/testfile.txt", data);
