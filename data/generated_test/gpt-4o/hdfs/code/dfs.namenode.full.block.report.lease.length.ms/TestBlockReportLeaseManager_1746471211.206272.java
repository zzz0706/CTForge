package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class TestBlockReportLeaseManager {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testPruneExpiredPending_removesExpiredLeases() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, 300000); // Set for test config
        long leaseLengthMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

        // Instantiate the BlockReportLeaseManager with the test Configuration
        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(conf);

        // 2. Prepare the test conditions.
        // Simulate multiple DataNode registrations and lease requests.
        DatanodeID dnId1 = new DatanodeID("127.0.0.1", "host1", "storageID1", 5000, 5001, 5002, 5003);
        DatanodeID dnId2 = new DatanodeID("127.0.0.2", "host2", "storageID2", 5000, 5001, 5002, 5003);
        DatanodeID dnId3 = new DatanodeID("127.0.0.3", "host3", "storageID3", 5000, 5001, 5002, 5003);

        DatanodeDescriptor dn1 = new DatanodeDescriptor(dnId1);
        DatanodeDescriptor dn2 = new DatanodeDescriptor(dnId2);
        DatanodeDescriptor dn3 = new DatanodeDescriptor(dnId3);

        leaseManager.requestLease(dn1);
        leaseManager.requestLease(dn2);
        leaseManager.requestLease(dn3);

        // Simulate time passing to create expired leases
        long simulatedCurrentTime = Time.monotonicNow() + leaseLengthMs + 1;

        // 3. Test code.
        // Invoke the pruneExpiredPending method using reflection
        Method pruneExpiredPendingMethod = BlockReportLeaseManager.class.getDeclaredMethod("pruneExpiredPending", long.class);
        pruneExpiredPendingMethod.setAccessible(true);
        pruneExpiredPendingMethod.invoke(leaseManager, simulatedCurrentTime);

        // Access private field pendingHead using reflection
        Field pendingHeadField = BlockReportLeaseManager.class.getDeclaredField("pendingHead");
        pendingHeadField.setAccessible(true);
        Object pendingHead = pendingHeadField.get(leaseManager);

        // Check that the pending list is empty (pendingHead.next points to itself)
        Field nextField = pendingHead.getClass().getDeclaredField("next");
        nextField.setAccessible(true);
        Object next = nextField.get(pendingHead);

        // Assert the pendingHead.next points to itself indicating the list is empty
        Assert.assertTrue("Pending leases should be empty after pruning expired entries.", next == pendingHead);

        // 4. Code after testing.
        // No additional cleanup necessary as the test does not modify static or persistent state.
    }
}