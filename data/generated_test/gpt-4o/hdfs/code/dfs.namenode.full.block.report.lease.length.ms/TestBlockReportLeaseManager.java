package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import java.lang.reflect.Field;

public class TestBlockReportLeaseManager {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testBlockReportLeaseManager_initialization() throws Exception {
        // Step 1: Prepare the test configuration
        Configuration conf = new Configuration();
        long leaseExpiry = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
            DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT // Default value to use if unset
        );

        // Ensure configuration value for the test is valid, it should be > 0
        Assert.assertTrue("Configured lease expiry must be greater than 0", leaseExpiry > 0);

        // Step 2: Instantiate BlockReportLeaseManager
        BlockReportLeaseManager leaseManager = new BlockReportLeaseManager(conf);

        // Step 3: Validate the initialization of critical fields using reflection due to private access
        Field leaseExpiryMsField = BlockReportLeaseManager.class.getDeclaredField("leaseExpiryMs");
        leaseExpiryMsField.setAccessible(true);
        long leaseExpiryMs = leaseExpiryMsField.getLong(leaseManager);

        Assert.assertEquals("The leaseExpiryMs field should reflect the configuration value",
            leaseExpiry, leaseExpiryMs);

        Field maxPendingField = BlockReportLeaseManager.class.getDeclaredField("maxPending");
        maxPendingField.setAccessible(true);
        int maxPending = maxPendingField.getInt(leaseManager);

        // maxPending is also initialized when the leaseManager is created. Ensure it uses a valid default value.
        int maxPendingDefault = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
            DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT
        );
        Assert.assertTrue("The maxPending value must be greater than or equal to 1", maxPending >= 1);
    }
}