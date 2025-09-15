package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBlockReportLeaseManagerConfigValidation {

  @Test
  public void testFullBlockReportLeaseLengthMsValidation() {
    // 1. Obtain configuration value via the HDFS 2.8.5 API
    Configuration conf = new Configuration();
    long leaseLengthMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
        DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

    // 2. Prepare the test conditions
    //    (none needed; we only read the existing value)

    // 3. Test code: validate the value against the documented constraints
    //    From BlockReportLeaseManager(BlockReportLeaseManager.java:74-83):
    //    Preconditions.checkArgument(leaseExpiryMs >= 1,
    //        "Cannot set full block report lease expiry period to a value less than 1.");
    assertTrue("dfs.namenode.full.block.report.lease.length.ms must be >= 1",
               leaseLengthMs >= 1);

    // 4. Code after testing
    //    (nothing to tear down)
  }
}