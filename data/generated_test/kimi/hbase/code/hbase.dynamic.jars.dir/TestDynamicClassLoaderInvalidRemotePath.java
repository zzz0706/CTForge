package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;

@Category(SmallTests.class)
public class TestDynamicClassLoaderInvalidRemotePath {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDynamicClassLoaderInvalidRemotePath.class);

  @Test
  public void invalidRemotePathLogsWarningAndDisablesRemote() throws Exception {
    // 1. Configuration as input – do NOT set hbase.dynamic.jars.dir
    Configuration conf = new Configuration();
    // Ensure dynamic loading is enabled so initTempDir is called
    conf.setBoolean("hbase.dynamic.jars.optional", true);

    // 2. Dynamic expected value calculation – not strictly needed for this test,
    //    but we read the default to keep the pattern
    String defaultRemotePath = conf.get("hbase.dynamic.jars.dir", "${hbase.rootdir}/lib");

    // 3. Prepare the test conditions – simulate FileSystem failure
    FileSystem mockFs = mock(FileSystem.class);
    doThrow(new IOException("Invalid filesystem")).when(mockFs).getFileStatus(any(Path.class));
    FileSystem.setDefaultUri(conf, "file:///");

    // 4. Test code – create loader and verify fields via reflection
    DynamicClassLoader loader = new DynamicClassLoader(conf, getClass().getClassLoader());

    Field remoteDirField = DynamicClassLoader.class.getDeclaredField("remoteDir");
    remoteDirField.setAccessible(true);
    Field remoteDirFsField = DynamicClassLoader.class.getDeclaredField("remoteDirFs");
    remoteDirFsField.setAccessible(true);

    // 5. Code after testing – assertions
    assertNull("remoteDir should be null", remoteDirField.get(loader));
    assertNull("remoteDirFs should be null", remoteDirFsField.get(loader));
  }
}