package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;

public class ChecksumFileSystemConfigTest {

  @Test
  public void testDefaultBufferSizeUsedWhenKeyAbsent() throws IOException {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    int expectedBufferSize = conf.getInt(
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);

    // 2. Prepare the test conditions.
    LocalFileSystem lfs = new LocalFileSystem();
    lfs.initialize(LocalFileSystem.getDefaultUri(new Configuration()), conf);

    // Create the test file before attempting to open it
    Path testFile = new Path("/tmp/testfile");
    try (OutputStream os = lfs.create(testFile)) {
      os.write("test data".getBytes());
    }

    // 3. Test code.
    FSDataInputStream in = lfs.open(testFile);

    // 4. Code after testing.
    in.close();
    lfs.close();
  }
}