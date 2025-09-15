package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChecksumFileSystemConfigTest {

  @Test
  public void testCustomBufferSizeIsUsed() throws IOException {
    // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    // 2. Prepare the test conditions.
    conf.setInt("io.file.buffer.size", 8192);
    LocalFileSystem lfs = FileSystem.getLocal(conf);
    lfs.initialize(LocalFileSystem.getDefaultUri(conf), conf);

    // 3. Test code.
    Path testPath = new Path("/tmp/testCustomBuffer");
    try (OutputStream os = lfs.create(testPath)) {
      os.write(new byte[]{0, 1});
    }

    // 4. Code after testing.
    lfs.delete(testPath, false);
    lfs.close();
  }
}