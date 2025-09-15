package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestSeqFileCompressBlockSizeConfig {

  @Test
  public void testValidCompressBlockSize() {
    Configuration conf = new Configuration(false);
    // Do NOT set any value – read whatever is in the configuration files
    int blockSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY,
        CommonConfigurationKeysPublic.IO_SEQFILE_COMPRESS_BLOCKSIZE_DEFAULT);

    // Constraint: block size must be a positive integer
    assertTrue("io.seqfile.compress.blocksize must be > 0",
               blockSize > 0);
  }
}