import Options
from os import unlink, symlink
from os.path import exists, abspath
import os

def set_options(opt):
  opt.tool_options("compiler_cxx")

def configure(conf):
  conf.check_tool("compiler_cxx")
  conf.check_tool("node_addon")
  # conf.check_cc(lib='libhdfs',  uselib_store='LIBHDFS',  mandatory=True)
  # conf.check_cfg(package='libhdfs0-0.20.2+923.21-1~lucid-cdh3', args='--cflags --libs', uselib_store='LIBHDFS')
  # conf.check_cfg(package='libnotifymm-1.0', args='--cflags --libs', uselib_store='LIBNOTIFYMM')

def build(bld):
  # obj = bld.new_task_gen("cxx", "shlib", "node_addon", includes='./src ./vendor /usr/lib/jvm/java-6-sun/include /usr/lib/jvm/java-6-sun/include/linux', linkflags=['-L/home/paul/src/hadoop-0.20.2-cdh3u0/c++/Linux-i386-32/lib', '-L/usr/lib/jvm/java-6-sun/jre/lib/i386/server', '-lhdfs'])
  obj = bld.new_task_gen("cxx", "shlib", "node_addon", includes='./src ./vendor', linkflags=['-lhdfs'])
  obj.cxxflags = ["-g", "-D_FILE_OFFSET_BITS=64", "-D_LARGEFILE_SOURCE", "-Wall"]
  obj.target = "hdfs_bindings"
  obj.source = "src/hdfs_bindings.cc"

def shutdown():
  if Options.commands['clean']:
    if exists('hdfs_bindings.node'): unlink('hdfs_bindings.node')
  else:
    if exists('build/default/hdfs_bindings.node') and not exists('hdfs_bindings.node'):
      symlink('build/default/hdfs_bindings.node', 'hdfs_bindings.node')
