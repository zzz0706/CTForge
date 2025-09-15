package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMaxXAttrsPerInodeConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new HdfsConfiguration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testMaxXAttrsPerInodeNonNegative() {
    int maxXAttrs = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

    assertTrue(
        "dfs.namenode.fs-limits.max-xattrs-per-inode must be >= 0",
        maxXAttrs >= 0);
  }

  @Test
  public void testMaxXAttrsPerInodeDefaultValue() {
    int maxXAttrs = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

    assertEquals(
        "Default value of dfs.namenode.fs-limits.max-xattrs-per-inode should be 32",
        32, maxXAttrs);
  }
}