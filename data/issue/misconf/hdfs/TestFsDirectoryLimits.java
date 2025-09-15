package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;
//HDFS-7473
public class TestFsDirectoryLimits {

  @Test
  public void testMaxDirectoryItemsRange() {
    HdfsConfiguration conf = new HdfsConfiguration();
    // Retrieve the key and its default value
    String key = DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY;
    int defaultVal = DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT;
    // Get the configured value (or default if not set)
    int maxItems = conf.getInt(key, defaultVal);

    // Assert that the value is between 1 and 6400000 (inclusive)
    assertTrue(
      String.format("Configuration %s=%d is out of valid range [1, 6400000]", key, maxItems),
      maxItems >= 1 && maxItems <= 6400000
    );
  }
}
