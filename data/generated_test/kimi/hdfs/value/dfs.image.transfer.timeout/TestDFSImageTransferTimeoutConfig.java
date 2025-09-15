package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestDFSImageTransferTimeoutConfig {

  @Test
  public void testDFSImageTransferTimeoutValid() {
    // 1. Obtain configuration from HDFS configuration files (no hard-coding)
    Configuration conf = new HdfsConfiguration();

    // 2. Read the configured value
    int timeout = conf.getInt(
        DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
        DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT);

    // 3. Validate the value
    //    - Must be an integer
    //    - Must be positive (0 is allowed to mean “use default”, but negative is invalid)
    assertTrue("dfs.image.transfer.timeout must be an integer", 
               Integer.toString(timeout).matches("\\d+"));
    assertTrue("dfs.image.transfer.timeout must be non-negative", 
               timeout >= 0);

    // 4. (Optional) Log for debugging
    // System.out.println("dfs.image.transfer.timeout = " + timeout);
  }
}