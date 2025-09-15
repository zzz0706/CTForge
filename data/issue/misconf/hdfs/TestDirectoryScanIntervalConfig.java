package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;
//HDFS-3407
public class TestDirectoryScanIntervalConfig {


  @Test
  public void testDirectoryScanIntervalValueRange() {
    HdfsConfiguration conf = new HdfsConfiguration();
    String key = DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY;
    int defaultVal = DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT;
    int interval = conf.getInt(key, defaultVal);

    // Assert that the interval is either < 0 (disabled) or >= 1 (seconds)
    assertTrue(
      String.format("Configuration %s=%d is invalid; expected < 0 or >= 1", key, interval),
      interval < 0 || interval >= 1
    );
  }
}
