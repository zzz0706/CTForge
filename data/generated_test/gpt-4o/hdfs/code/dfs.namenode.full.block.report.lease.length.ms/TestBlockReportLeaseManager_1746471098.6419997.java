package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

public class TestBlockReportLeaseManager {

    @Test
    // Test checkLease method for an expired lease.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckLease_expiredLease() {
        // 1. Obtain configuration values using the API.
        Configuration conf = new Configuration();
        long defaultLeaseExpiryMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

        // Set a small lease expiry time for testing purpose.
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, 10);

        // 2. Prepare the test conditions.
        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(conf);

        // Correctly instantiate DatanodeDescriptor using DatanodeID.
        DatanodeID datanodeID = new DatanodeID("127.0.0.1", "localhost", "test-datanode-uuid", 1000, 2000, 3000, 4000);
        DatanodeDescriptor dataNode = new DatanodeDescriptor(datanodeID);

        long leaseId = leaseManager.requestLease(dataNode); // Request a lease.

        // Simulate a delay exceeding the lease expiry time by manually adjusting monotonic time.
        long delayedTimeMs = Time.monotonicNow() + 20; // Exceed the lease expiry threshold.

        // 3. Test code.
        boolean isLeaseValid = leaseManager.checkLease(dataNode, delayedTimeMs, leaseId);

        // 4. Code after testing.
        Assert.assertFalse("checkLease should return false for an expired lease.", isLeaseValid);
    }
}