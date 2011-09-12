# node-hdfs

This provides a JNI-implemented HDFS client allowing node.js applications to natively interoperate with the Hadoop FileSystem.

## Example Use

There's a run script that will build + run the `demo/demo.js` sample. 

    var HDFS = require('node-hdfs');
    var client = new HDFS({host:"default", port:0});

    client.list("/tmp/path", function(err, files) {
      console.log(files);
    });

## Compiling

At the moment it's still a little tricky. At the least you'll need to make sure `libhdfs` is built and installed in a path accessible by ldconfig (i.e. /usr/local/lib).

### Ubuntu

I (Paul) have had success with Cloudera's CDH distribution. Please see [their instructions for installing from packages](https://ccp.cloudera.com/display/CDHDOC/CDH3+Installation).

### Mac OSX

    cd [hadoop_uncompressed_src_path]/src/c++/libhdfs
    chmod +x ./configure
    ./configure

To build libhdfs in Mac OSX a few changes need to be done in the generated 'Makefile' by './configure':

Edit Makefile and search for a line that starts with `'CFLAGS = -g O2'`
* Remove the parameter `"-m"`
* Add parameter `"-framework JavaVM"`

Search for a line that starts with `'DEFS = -DPACKAGE_NAME=\"libhdfs\" -DPACKAGE_TARNAME=\"libhdfs\"'`
* Remove the parameter `"-Dsize_t=unsigned\ int"`

Comment or remove include in `'hdfsJniHelper.c'` :

Search for `'#include <error.h>'` and remove (or comment it out).

Then build

    make
    chmod +x ./install-sh
    make install

Copy the libraries to a folder accessible to the compiler or add it to the env with dyld (man dyld for more information).

    cd [hadoop_uncompressed_src_path]/src/c++/install/lib
    cp libhdfs* /usr/local/lib
