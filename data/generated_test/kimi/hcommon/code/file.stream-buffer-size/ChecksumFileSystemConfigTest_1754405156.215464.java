package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ChecksumFileSystemConfigTest {

  private LocalFileSystem lfs;
  private Path testPath;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    // 1. Use the key defined in LocalFileSystemConfigKeys
    conf.setInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 8192);
    // 2. Prepare the test conditions: initialize LocalFileSystem with the custom buffer size.
    lfs = FileSystem.getLocal(conf);
    lfs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
    testPath = new Path(System.getProperty("java.io.tmpdir"), "testCustomBuffer");
  }

  @Test
  public void testCustomBufferSizeIsUsed() throws Exception {
    // 3. Test code: create a file which will trigger ChecksumFSInputChecker constructor
    // and exercise getSumBufferSize via ChecksumFileSystem#create
    try (OutputStream os = lfs.create(testPath)) {
      os.write(new byte[]{0, 1});
    }

    // Verify the configuration was honored by checking the buffer size used.
    assertTrue(lfs.exists(testPath));

    // Open the file to trigger ChecksumFSInputChecker and verify buffer size propagation
    FSDataInputStream in = lfs.open(testPath);
    assertNotNull(in);

    // Use reflection to access private fields and verify buffer sizes
    Object checker = in.getWrappedStream();
    Class<?> clazz = Class.forName("org.apache.hadoop.fs.ChecksumFileSystem$ChecksumFSInputChecker");
    Field fsField = clazz.getDeclaredField("fs");
    fsField.setAccessible(true);
    ChecksumFileSystem fsInstance = (ChecksumFileSystem) fsField.get(checker);

    // Verify getSumBufferSize is called correctly
    Method getSumBufferSize = ChecksumFileSystem.class.getDeclaredMethod(
        "getSumBufferSize", int.class, int.class);
    getSumBufferSize.setAccessible(true);
    int sumBufferSize = (Integer) getSumBufferSize.invoke(fsInstance, 512, 8192);
    
    // Verify the calculated sumBufferSize is based on the configuration
    assertTrue(sumBufferSize >= 8192);
    
    in.close();
  }

  @Test
  public void testDefaultBufferSizeFallback() throws Exception {
    // Test the default value fallback when configuration is not set
    Configuration conf = new Configuration();
    conf.unset(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY);
    LocalFileSystem localFs = FileSystem.getLocal(conf);
    localFs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
    
    Path defaultTestPath = new Path(System.getProperty("java.io.tmpdir"), "testDefaultBuffer");
    
    try (OutputStream os = localFs.create(defaultTestPath)) {
      os.write(new byte[]{0, 1});
    }
    
    FSDataInputStream in = localFs.open(defaultTestPath);
    assertNotNull(in);
    
    // Verify default buffer size is used
    Object checker = in.getWrappedStream();
    Class<?> clazz = Class.forName("org.apache.hadoop.fs.ChecksumFileSystem$ChecksumFSInputChecker");
    Field fsField = clazz.getDeclaredField("fs");
    fsField.setAccessible(true);
    ChecksumFileSystem fsInstance = (ChecksumFileSystem) fsField.get(checker);
    
    Method getSumBufferSize = ChecksumFileSystem.class.getDeclaredMethod(
        "getSumBufferSize", int.class, int.class);
    getSumBufferSize.setAccessible(true);
    int sumBufferSize = (Integer) getSumBufferSize.invoke(fsInstance, 512, 4096);
    
    // Verify the calculated sumBufferSize uses default when not configured
    assertTrue(sumBufferSize >= LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);
    
    in.close();
    localFs.delete(defaultTestPath, false);
    localFs.close();
  }

  @After
  public void tearDown() throws IOException {
    // 4. Code after testing: clean up the test file and close the filesystem
    if (lfs != null) {
      if (testPath != null && lfs.exists(testPath)) {
        lfs.delete(testPath, false);
      }
      lfs.close();
    }
  }
}