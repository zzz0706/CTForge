package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDFSReplicationIntervalConfig {

  @Test
  public void testReplicationIntervalPositive() {
    Configuration conf = new Configuration();


    long interval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT);

    assertTrue("dfs.namenode.replication.interval must be > 0", interval > 0);
  }
}