package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({ MiscTests.class, SmallTests.class })
public class TestDynamicClassLoaderCustomRemoteDir {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDynamicClassLoaderCustomRemoteDir.class);

  @Test
  public void customRemoteDirInitializesFilesystem() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    // Read the default so we can compute the local path that the loader will build
    String defaultRoot = conf.get("hbase.rootdir", "file:///tmp/hbase");
    String defaultLocalDir = conf.get("hbase.local.dir", "/tmp/hbase-local");
    String expectedLocalPath = defaultLocalDir + "/lib";

    // 2. Dynamic expected value calculation
    String remotePath = "file:///tmp/hbase/lib";
    conf.set("hbase.dynamic.jars.dir", remotePath);

    // 3. Invoke the method under test
    DynamicClassLoader loader = new DynamicClassLoader(conf, ClassLoader.getSystemClassLoader());

    // 4. Assertions and verification
    java.lang.reflect.Field remoteDirField = DynamicClassLoader.class.getDeclaredField("remoteDir");
    remoteDirField.setAccessible(true);
    Path actualRemoteDir = (Path) remoteDirField.get(loader);

    java.lang.reflect.Field remoteDirFsField = DynamicClassLoader.class.getDeclaredField("remoteDirFs");
    remoteDirFsField.setAccessible(true);
    FileSystem actualRemoteDirFs = (FileSystem) remoteDirFsField.get(loader);

    assertEquals(new Path(remotePath), actualRemoteDir);
    assertNotNull(actualRemoteDirFs);
  }
}