package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

public class TestBlockReportLeaseManager {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testPruneExpiredPending_removesExpiredLeases() throws Exception {
        // 1. Prepare the Configuration object and set up necessary values.
        Configuration conf = new Configuration();
        long leaseLengthMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(conf);

        // 2. Simulate multiple DataNode registrations and lease requests.
        DatanodeID dnId1 = new DatanodeID("127.0.0.1", "host1", "storage1", 5000, 5001, 5002, 5003);
        DatanodeID dnId2 = new DatanodeID("127.0.0.2", "host2", "storage2", 5000, 5001, 5002, 5003);
        DatanodeID dnId3 = new DatanodeID("127.0.0.3", "host3", "storage3", 5000, 5001, 5002, 5003);

        DatanodeDescriptor dn1 = new DatanodeDescriptor(dnId1);
        DatanodeDescriptor dn2 = new DatanodeDescriptor(dnId2);
        DatanodeDescriptor dn3 = new DatanodeDescriptor(dnId3);
        
        // Request leases for each DataNode.
        leaseManager.requestLease(dn1);
        leaseManager.requestLease(dn2);
        leaseManager.requestLease(dn3);

        // Simulate a delay to cause some leases to expire. Use reflection to access private methods.
        long simulatedCurrentTime = Time.monotonicNow() + leaseLengthMs + 1;

        // Use reflection to access the private pruneExpiredPending(long) method.
        java.lang.reflect.Method pruneExpiredPendingMethod =
                leaseManager.getClass().getDeclaredMethod("pruneExpiredPending", long.class);
        pruneExpiredPendingMethod.setAccessible(true);
        pruneExpiredPendingMethod.invoke(leaseManager, simulatedCurrentTime);

        // 4. Verify that expired leases are removed.
        // Use reflection to access the private pending data structure.
        java.lang.reflect.Field pendingHeadField = leaseManager.getClass().getDeclaredField("pendingHead");
        pendingHeadField.setAccessible(true);
        Object pendingHead = pendingHeadField.get(leaseManager);

        java.lang.reflect.Field nextField = pendingHead.getClass().getDeclaredField("next");
        nextField.setAccessible(true);
        Object next = nextField.get(pendingHead);

        Assert.assertTrue("Pending leases should be empty after pruning expired entries.", next == pendingHead);

        // No additional cleanup needed as the test has concluded.
    }
}