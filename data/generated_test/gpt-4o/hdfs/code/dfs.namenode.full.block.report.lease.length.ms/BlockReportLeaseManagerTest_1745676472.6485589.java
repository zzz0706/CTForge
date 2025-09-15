package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class BlockReportLeaseManagerTest {

    /**
     * Test the behavior of requestLease when numPending reaches maxPending, including configuration usage.
     */
    @Test
    public void test_requestLease_exceedMaxPending_withConfigUsage() {
        // Step 1: Get configuration value using API
        Configuration conf = new Configuration();
        conf.setInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
                1); // Set maxPending to 1 via configuration
        conf.setLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                5000); // Set lease expiry to 5000ms via configuration

        long leaseExpiryMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

        // Step 2: Prepare the input conditions for unit testing
        int maxPending = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT);

        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(maxPending, leaseExpiryMs);

        DatanodeDescriptor datanode1 = new DatanodeDescriptor(
                new DatanodeID("127.0.0.1", "localhost", "storage1", 1234, 1234, 1234, 1234));
        DatanodeDescriptor datanode2 = new DatanodeDescriptor(
                new DatanodeID("127.0.0.2", "localhost", "storage2", 2234, 2234, 2234, 2234));

        // Step 3: Issue a lease request for the first datanode
        long leaseId1 = leaseManager.requestLease(datanode1);

        // Assert that the first lease is successfully issued
        assertNotEquals("Lease request for datanode1 should succeed.", 0, leaseId1);

        // Step 4: Attempt another lease request for the second datanode
        long leaseId2 = leaseManager.requestLease(datanode2);

        // Assert that the second lease request does not lead to a lease issuance
        assertEquals("Lease request for datanode2 should fail due to maxPending constraint.", 0, leaseId2);
    }
}