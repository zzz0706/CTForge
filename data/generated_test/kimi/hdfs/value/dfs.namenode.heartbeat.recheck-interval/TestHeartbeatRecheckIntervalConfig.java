package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHeartbeatRecheckIntervalConfig {

  @Test
  public void testStaleDatanodeIntervalNotLessThanRecheckInterval() {
    Configuration conf = new Configuration();

    long recheckInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT);
    long staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);

    if (staleInterval < recheckInterval) {
      staleInterval = recheckInterval;
    }
    assertTrue(
        "Invalid configuration: " +
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY +
        " (" + staleInterval + " ms) must be greater than or equal to " +
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY +
        " (" + recheckInterval + " ms)",
        staleInterval >= recheckInterval);
  }
}