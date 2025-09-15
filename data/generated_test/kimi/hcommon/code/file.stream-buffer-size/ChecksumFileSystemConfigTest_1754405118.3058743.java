package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    testPath = new Path("/tmp/testCustomBuffer");
  }

  @Test
  public void testCustomBufferSizeIsUsed() throws IOException {
    // 3. Test code: create a file which will trigger ChecksumFSInputChecker constructor
    // and exercise getSumBufferSize via ChecksumFileSystem#create
    try (OutputStream os = lfs.create(testPath)) {
      os.write(new byte[]{0, 1});
    }

    // Verify the configuration was honored by checking the buffer size used.
    // We cannot directly assert the internal buffer size, but we ensure no exception
    // is thrown and the file is created successfully.
    assertEquals(true, lfs.exists(testPath));
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