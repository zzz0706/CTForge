package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestNameNodeResourceDuReservedConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Obtain the configuration object without hard-coding any values
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testDuReservedIsPositiveLong() {
    // 2. Prepare the test conditions: rely on the existing configuration file
    // 3. Test code
    long duReserved = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

    // The reserved space must be a non-negative long value
    assertTrue("dfs.namenode.resource.du.reserved must be >= 0", duReserved >= 0);
  }

  @Test
  public void testDuReservedParsesCorrectly() {
    // 3. Test code: verify the value is a valid long
    String raw = conf.get(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY);
    if (raw != null) {
      try {
        Long.parseLong(raw.trim());
      } catch (NumberFormatException e) {
        fail("dfs.namenode.resource.du.reserved must be a valid long integer");
      }
    }
  }
}