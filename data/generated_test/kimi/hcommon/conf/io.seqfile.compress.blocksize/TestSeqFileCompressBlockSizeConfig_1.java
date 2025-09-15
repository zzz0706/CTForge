package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestSeqFileCompressBlockSizeConfig {

  @Test
  public void testSeqFileCompressBlockSizeValue() {
    // 1. Use the hdfs 2.8.5 API to obtain configuration values
    Configuration conf = new Configuration();
    int blockSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY,
        1000000);

    // 2. Prepare the test conditions — none needed, we just validate the loaded value

    // 3. Test code
    // The configuration must be a positive integer (>=1)
    assertTrue("io.seqfile.compress.blocksize must be a positive integer",
               blockSize > 0);

    // 4. Code after testing — none needed
  }
}