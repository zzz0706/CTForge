package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertFalse;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
//HBASE-22531
public class TestUseHeap {

  @Test
  public void disabledBlockCache_shouldReturnFalse() throws Exception {
   
    Configuration conf = new Configuration(false);
    conf.setFloat("hfile.block.cache.size", 0.0f);
    conf.unset("hbase.bucketcache.ioengine");
    CacheConfig cacheConf = new CacheConfig(conf);

    Object reader = allocateWithoutCtor(HFileReaderImpl.class);
    Field f = HFileReaderImpl.class.getDeclaredField("cacheConf");
    f.setAccessible(true);
    f.set(reader, cacheConf);

    Class<?> blockTypeClass = resolveBlockTypeClass();
    Object DATA = getEnumConstant(blockTypeClass, "DATA");

    Method m = HFileReaderImpl.class.getDeclaredMethod("shouldUseHeap", blockTypeClass);
    m.setAccessible(true);
    boolean useHeap = (boolean) m.invoke(reader, DATA);

    assertFalse("When BlockCache is disabled, shouldUseHeap must be false.", useHeap);
  }

  private static Class<?> resolveBlockTypeClass() throws ClassNotFoundException {
    try {
      return Class.forName("org.apache.hadoop.hbase.io.hfile.BlockType");
    } catch (ClassNotFoundException e) {
      return Class.forName("org.apache.hadoop.hbase.io.hfile.HFileBlock$BlockType");
    }
  }

  private static Object getEnumConstant(Class<?> enumClass, String name) throws Exception {
    try {
      Field fld = enumClass.getField(name);
      fld.setAccessible(true);
      return fld.get(null);
    } catch (NoSuchFieldException ignore) {
      @SuppressWarnings("unchecked")
      Class<? extends Enum> ec = (Class<? extends Enum>) enumClass;
      return Enum.valueOf(ec, name);
    }
  }

  private static Object allocateWithoutCtor(Class<?> cls) throws Exception {
    Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
    f.setAccessible(true);
    sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
    return unsafe.allocateInstance(cls);
  }
}
