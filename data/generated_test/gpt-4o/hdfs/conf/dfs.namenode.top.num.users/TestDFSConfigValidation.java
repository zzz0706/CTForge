package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDFSConfigValidation {

  @Test
  public void testDFSNameNodeTopNumUsersConfiguration() {
    // Prepare test conditions: Create a Configuration object to read values from.
    Configuration conf = new Configuration();

    // Retrieve the configuration value using HDFS APIs.
    int topUsersCnt = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);

    // Test code: Validate the configuration based on constraints.
    // 1. Check that "dfs.namenode.top.num.users" is greater than 0.
    assertTrue("The number of requested top users must be at least 1", topUsersCnt > 0);

    // Code after testing: Optionally, additional cleanup or logging.
    // For example, logging the value retrieved to ensure it's being tested correctly.
    System.out.println("Test passed for `dfs.namenode.top.num.users` with value: " + topUsersCnt);
  }
}