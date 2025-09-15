package org.apache.hadoop.hdfs.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class TestDfsClientBlockWriteRetriesConfig {

  @Test
  public void testBlockWriteRetriesConfigValidity() {
    // 1. Obtain the configuration value via the HDFS 2.8.5 API
    Configuration conf = new Configuration();
    int retries = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY,
        DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT);

    // 2. Prepare test conditions: no additional setup needed; we only validate the retrieved value

    // 3. Test code: validate the configuration value
    //    The configuration represents a retry count, so it must be a non-negative integer.
    if (retries < 0) {
      fail("Invalid configuration value for " +
           DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY +
           ": " + retries + " (must be >= 0)");
    }

    // 4. Code after testing: assert the value is valid
    assertTrue("Configuration value must be non-negative",
               retries >= 0);
  }
}