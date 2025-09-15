package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;

public class TestBlockReportLeaseManager {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testRequestLease_hitsMaxPendingLimit() {
        // Step 1: Initialize Configuration and set the property
        Configuration conf = new Configuration();
        long leaseExpiryMs = conf.getLong(
                "dfs.namenode.full.block.report.lease.length.ms",
                300000); // Getting value using Hadoop API

        // Step 2: Initialize BlockReportLeaseManager with maxPending set to 1
        BlockReportLeaseManager leaseManager =
                new BlockReportLeaseManager(1, leaseExpiryMs); // maxPending = 1

        // Step 3: Create two DatanodeDescriptors
        // Constructing DatanodeDescriptor using DatanodeID, adhering to the API of hdfs 2.8.5
        DatanodeID datanodeId1 = new DatanodeID("127.0.0.1", "localhost", "datanode-1", 50010, 50020, 50070, 9864);
        DatanodeDescriptor dn1 = new DatanodeDescriptor(datanodeId1, NetworkTopology.DEFAULT_RACK);

        DatanodeID datanodeId2 = new DatanodeID("127.0.0.2", "localhost", "datanode-2", 50010, 50020, 50070, 9864);
        DatanodeDescriptor dn2 = new DatanodeDescriptor(datanodeId2, NetworkTopology.DEFAULT_RACK);

        // Step 4: Request lease for the first DataNode
        long leaseId1 = leaseManager.requestLease(dn1);
        assert leaseId1 != 0; // First lease should succeed

        // Step 5: Attempt to request a lease for the second DataNode
        long leaseId2 = leaseManager.requestLease(dn2);
        assert leaseId2 == 0; // Second lease should fail due to maxPending limit

        // Step 6: Code after testing; cleanup or additional verifications if required
        // (Logs for denial should be manually verified if the logger output is configured properly)
    }
}