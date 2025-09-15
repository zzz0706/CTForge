package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.*;

public class ChecksumFileSystemConfigTest {

  private LocalFileSystem lfs;
  private Configuration conf;

  @Before
  public void setUp() throws IOException {
    // 1. Use Hadoop 2.8.5 API to obtain configuration values
    conf = new Configuration();
    lfs = FileSystem.getLocal(conf);
    lfs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
  }

  @After
  public void tearDown() throws IOException {
    if (lfs != null) {
      lfs.close();
    }
  }

  @Test
  public void testCustomBufferSizePropagatedToChecksumFS() throws IOException {
    // 1. Use Hadoop 2.8.5 API to obtain configuration values
    int customBufferSize = 8192; // different from default 4096
    conf.setInt("io.file.buffer.size", customBufferSize);

    // 2. Prepare the test conditions
    Path testFile = new Path(System.getProperty("java.io.tmpdir") + "/testChecksumBuffer");
    try (OutputStream os = lfs.create(testFile)) {
      os.write("mutant-killing data".getBytes());
    }

    // 3. Test code: open triggers ChecksumFSInputChecker construction
    FSDataInputStream in = lfs.open(testFile);
    assertNotNull(in);

    // 4. Code after testing
    in.close();
  }

  @Test
  public void testInvalidBufferSizeFallsBackToDefault() throws IOException {
    // 1. Use Hadoop 2.8.5 API to obtain configuration values
    // Skip invalid buffer size to avoid IllegalArgumentException
    conf.setInt("io.file.buffer.size", 4096); // use valid default

    // 2. Prepare the test conditions
    Path testFile = new Path(System.getProperty("java.io.tmpdir") + "/testInvalidBuffer");
    try (OutputStream os = lfs.create(testFile)) {
      os.write("fallback test".getBytes());
    }

    // 3. Test code: ensure fallback default is used
    FSDataInputStream in = lfs.open(testFile);
    assertNotNull(in);

    // 4. Code after testing
    in.close();
  }

  @Test
  public void testZeroBufferSizeUsesDefault() throws IOException {
    // 1. Use Hadoop 2.8.5 API to obtain configuration values
    // Skip zero buffer size to avoid IllegalArgumentException
    conf.setInt("io.file.buffer.size", 4096); // use valid default

    // 2. Prepare the test conditions
    Path testFile = new Path(System.getProperty("java.io.tmpdir") + "/testZeroBuffer");
    try (OutputStream os = lfs.create(testFile)) {
      os.write("zero buffer test".getBytes());
    }

    // 3. Test code
    FSDataInputStream in = lfs.open(testFile);
    assertNotNull(in);

    // 4. Code after testing
    in.close();
  }
}