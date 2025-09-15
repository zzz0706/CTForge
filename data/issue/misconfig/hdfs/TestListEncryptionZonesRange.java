package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;
//HDFS-6814
public class TestListEncryptionZonesRange {

  @Test
  public void testEncryptionZonesNumResponsesRange() {
    HdfsConfiguration conf = new HdfsConfiguration();
    // the property name and its default
    String key = DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES;
    int defaultVal = DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT;
    // retrieve the configured (or default) value
    int val = conf.getInt(key, defaultVal);

    // assert that it's at least 1
    assertTrue(
      String.format("Configuration %s=%d should be >= 1", key, val),
      val >= 1
    );
  }
}
