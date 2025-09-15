package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestDFSImageTransferConfiguration {

    @Test
    // Test code to validate 'dfs.image.transfer.bandwidthPerSec' configuration.
    public void testDFSImageTransferBandwidthPerSecConfiguration() {
        // 1. Prepare the test conditions.
        // Load the HDFS configuration without setting values manually.
        Configuration conf = new Configuration();

        // Verify that the configuration value exists and is fetched correctly.
        long transferBandwidth = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT
        );

        // 2. Test code: Validate if the 'dfs.image.transfer.bandwidthPerSec' value
        // satisfies the constraints.

        // Check: Default value constraint - throttling disabled if transferBandwidth == 0.
        if (transferBandwidth == DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT) {
            assertTrue("Throttling is disabled as expected for the default value",
                    transferBandwidth == 0);
        } else {
            // Check: transferBandwidth must be greater than zero if throttling is enabled.
            assertTrue("Transfer bandwidth must be a positive value when throttling is enabled",
                    transferBandwidth > 0);

            // Also verify that the DataTransferThrottler is correctly instantiated.
            DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
            assertNotNull("Throttler must not be null when bandwidth is positive", throttler);
            assertTrue("Throttler should have the correct bandwidth set",
                    throttler.getBandwidth() > 0);

            // Additional Throttler validation based on internal constraints.
            long throttlerBandwidth = throttler.getBandwidth();
            assertTrue("Throttler bandwidth per second should match the configuration",
                    throttlerBandwidth == transferBandwidth);
        }

        // 3. Code after testing.
        // Clean up or perform any other final assertions (if necessary).
    }
}