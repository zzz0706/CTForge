package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFsDatasetCacheConfigValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Obtain the configuration object that will be populated from the
    //    test resource files (hdfs-site.xml, core-site.xml, etc.).
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testRevocationTimeoutMsConstraints() {
    // 2. Read the value from the configuration files (no hard-coding).
    long revocationMs = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS,
        DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT);

    long pollingMs = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS,
        DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS_DEFAULT);

    // 3. Validate the dependency: pollingMs must be ≤ revocationMs / 2.
    assertTrue(
        "Configured value " + pollingMs + " for " +
        DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS +
        " is too high.  It must not be more than half of the value of " +
        DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS + ".",
        pollingMs <= revocationMs / 2);

    // Validate non-negative requirement.
    assertTrue(
        DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS +
        " must be non-negative.",
        revocationMs >= 0);

    // 4. Nothing to clean up; assertions already performed.
  }
}