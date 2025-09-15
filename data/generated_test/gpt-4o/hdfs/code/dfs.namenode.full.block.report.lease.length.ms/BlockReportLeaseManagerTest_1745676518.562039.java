package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockReportLeaseManagerTest {

    // Test code to validate lease requests exceeding maxPending
    @Test
    public void test_requestLease_exceedMaxPending_withConfiguration() {
        // Step 1: Get configuration values using API
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES, 1); // Set maxPending to 1
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, 5000); // Set lease expiry to 5000ms

        // Parse configuration and prepare testing conditions
        long leaseExpiryMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

        int maxPending = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT);

        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(maxPending, leaseExpiryMs);

        DatanodeDescriptor datanode1 = new DatanodeDescriptor(
                new DatanodeID("127.0.0.1", "localhost", "storage1", 1234, 1234, 1234, 1234));
        DatanodeDescriptor datanode2 = new DatanodeDescriptor(
                new DatanodeID("127.0.0.2", "localhost", "storage2", 2234, 2234, 2234, 2234));

        // Step 2: Verify lease issuance under maxPending limit
        long leaseId1 = leaseManager.requestLease(datanode1);
        assertNotEquals("Lease request for datanode1 should succeed.", 0, leaseId1);

        // Step 3: Verify lease issuance fails when reaching maxPending
        long leaseId2 = leaseManager.requestLease(datanode2);
        assertEquals("Lease request for datanode2 should fail due to maxPending constraint.", 0, leaseId2);
    }

    @Test
    public void test_checkLease_withExpiredLease() {
        // Step 1: Get configuration value using API
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES, 2);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, 100);

        // Parse configuration
        long leaseExpiryMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);
        int maxPending = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT);

        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(maxPending, leaseExpiryMs);

        DatanodeDescriptor datanode1 = new DatanodeDescriptor(
                new DatanodeID("127.0.0.1", "localhost", "storage1", 1234, 1234, 1234, 1234));

        // Step 2: Issue a lease and validate it
        long leaseId = leaseManager.requestLease(datanode1);
        assertNotEquals("Lease request should create a valid lease ID", 0, leaseId);

        // Simulate a delay causing the lease to expire using Time.monotonicNow() utility
        long monotonicNowMs = Time.monotonicNow() + 200; // Exceed lease expiry time

        assertFalse("Expired lease should fail validation",
                leaseManager.checkLease(datanode1, monotonicNowMs, leaseId));
    }
}