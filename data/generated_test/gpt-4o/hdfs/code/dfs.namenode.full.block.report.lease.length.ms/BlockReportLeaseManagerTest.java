package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class BlockReportLeaseManagerTest {

    @Test
    public void test_requestLease_exceedMaxPending() {
        // 1. Use API to get configuration values rather than hardcoding them
        Configuration conf = new Configuration();
        long leaseExpiryMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

        // 2. Prepare test conditions
        int maxPending = 1; // Set maxPending to simulate the constraint
        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(maxPending, leaseExpiryMs);

        // Create mock DatanodeDescriptors to simulate lease requests
        DatanodeDescriptor datanode1 = new DatanodeDescriptor(new DatanodeID("127.0.0.1", "localhost", "storage1", 1234, 1234, 1234, 1234));
        DatanodeDescriptor datanode2 = new DatanodeDescriptor(new DatanodeID("127.0.0.2", "localhost", "storage2", 2234, 2234, 2234, 2234));

        // 3. Issue a lease request for the first datanode
        long leaseId1 = leaseManager.requestLease(datanode1);
        
        // Ensure the first lease is successfully issued
        assertNotEquals(0, leaseId1); 

        // 4. Attempt another lease request for the second datanode
        long leaseId2 = leaseManager.requestLease(datanode2);
        
        // Test assertion: Second lease request should fail, returning '0'
        assertEquals(0, leaseId2); 
    }
}