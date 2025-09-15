package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNameNodeResourceCheckedVolumesMinimumConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Use the hdfs 2.8.5 API to obtain configuration values without hard-coding them
    conf = new Configuration();
    // Ensure we do not override any user-supplied value in the test
    conf.clear();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testMinimumRedundantVolumesIsPositiveInt() throws IOException {
    // 2. Prepare test conditions: rely on the configuration file or defaults
    // 3. Test code: validate the value read from configuration
    int minimumRedundantVolumes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
        DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT);

    // Constraint: must be a positive integer (>= 1)
    if (minimumRedundantVolumes < 1) {
      fail("Configuration " + DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY
          + " must be a positive integer (>= 1). Found: " + minimumRedundantVolumes);
    }

    // 4. Code after testing: none required for this simple validation
  }
}