package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

import static org.junit.Assert.*;

public class ImageTransferBandwidthConfigTest {

    @Test
    // Test that when dfs.image.transfer.bandwidthPerSec is set to a positive value,
    // ImageServlet.getThrottler creates a DataTransferThrottler with the correct bandwidth
    // 1. Create a Configuration object and set dfs.image.transfer.bandwidthPerSec to a positive value
    // 2. Call ImageServlet.getThrottler with the configuration
    // 3. Verify that the returned throttler is not null and has the correct bandwidth
    // 4. Clean up (implicit at end of method)
    public void testBandwidthThrottlerCreatedWhenPositiveValue() {
        // Prepare configuration with positive bandwidth
        Configuration conf = new Configuration();
        long bandwidth = 1024L;
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, bandwidth);

        // Invoke ImageServlet.getThrottler method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Assert that the returned DataTransferThrottler instance is not null
        assertNotNull("DataTransferThrottler should be created when bandwidth is positive", throttler);

        // Assert that the throttler's getBandwidth() method returns the same value as the configuration
        assertEquals("Throttler bandwidth should match configured value", bandwidth, throttler.getBandwidth());
    }

    @Test
    // Test that when dfs.image.transfer.bandwidthPerSec is set to zero,
    // ImageServlet.getThrottler returns null (no throttling)
    // 1. Create a Configuration object and set dfs.image.transfer.bandwidthPerSec to 0
    // 2. Call ImageServlet.getThrottler with the configuration
    // 3. Verify that the returned throttler is null
    // 4. Clean up (implicit at end of method)
    public void testNoThrottlerWhenBandwidthZero() {
        // Prepare configuration with zero bandwidth
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0);

        // Invoke ImageServlet.getThrottler method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Assert that the returned DataTransferThrottler instance is null
        assertNull("DataTransferThrottler should not be created when bandwidth is zero", throttler);
    }

    @Test
    // Test that when dfs.image.transfer.bandwidthPerSec is not set,
    // ImageServlet.getThrottler uses the default value and behaves accordingly
    // 1. Create a Configuration object without setting dfs.image.transfer.bandwidthPerSec
    // 2. Get the default value directly from Configuration
    // 3. Call ImageServlet.getThrottler with the configuration
    // 4. Verify behavior based on the default value
    // 5. Clean up (implicit at end of method)
    public void testBandwidthConfigDefaultValue() {
        // Verify Configuration returns the default value
        Configuration conf = new Configuration(false); // Don't load default resources to test default value
        long configValue = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                                        DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);

        assertEquals("Default value should match DFSConfigKeys constant",
                     DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT, configValue);

        // Test ImageServlet.getThrottler with default configuration
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // If default is <= 0, no throttler should be created
        if (DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT <= 0) {
            assertNull("Throttler should not be created when default bandwidth is non-positive", throttler);
        } else {
            // If default is > 0, throttler should be created with default bandwidth
            assertNotNull("Throttler should be created when default bandwidth is positive", throttler);
            assertEquals("Throttler bandwidth should match default configured value",
                         DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT, throttler.getBandwidth());
        }
    }
}