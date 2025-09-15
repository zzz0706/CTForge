package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestImageServletConfigurationUsage {

    @Test
    // testGetThrottlerWithThrottlingDisabled
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottlerWithThrottlingDisabled() {
        // Prepare the test conditions: initialize Configuration and set throttling to disabled (0)
        Configuration configuration = new Configuration();
        configuration.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0);

        // Test code: Call getThrottler method
        DataTransferThrottler throttler = ImageServlet.getThrottler(configuration);

        // Code after testing: Validate the expected behavior
        assertNull("Throttler should be null when dfs.image.transfer.bandwidthPerSec is zero or negative", throttler);
    }

    @Test
    // testGetThrottlerWithPositiveThrottling
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottlerWithPositiveThrottling() {
        // Prepare the test conditions: initialize Configuration and set throttling to a positive value
        Configuration configuration = new Configuration();
        long transferRate = 1024 * 1024; // 1MB/sec
        configuration.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, transferRate);

        // Test code: Call getThrottler method
        DataTransferThrottler throttler = ImageServlet.getThrottler(configuration);

        // Code after testing: Validate the expected behavior
        assertNotNull("Throttler should not be null when dfs.image.transfer.bandwidthPerSec is positive", throttler);
        assertEquals("Throttler bandwidth should match the configured value", transferRate, throttler.getBandwidth());
    }

    @Test
    // testThrottleMethod
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testThrottleMethod() throws InterruptedException {
        // Prepare the test conditions: Create instance of DataTransferThrottler with a defined rate
        long transferRate = 1024; // 1 KB/sec
        DataTransferThrottler throttler = new DataTransferThrottler(transferRate);

        // Test code: Simulate a data transfer operation
        long numOfBytes = 2048; // Simulate transferring 2 KB
        long startTime = System.nanoTime(); // Use System.nanoTime for timing
        throttler.throttle(numOfBytes, null); // Call throttle method without a canceler

        // Code after testing: Validate the delay enforcing the bandwidth limit
        long durationMillis = (System.nanoTime() - startTime) / 1_000_000; // Convert nanoseconds to milliseconds
        long expectedDelayMillis = (numOfBytes * 1000) / transferRate; // Calculate expected delay in milliseconds
        assertTrue(
                String.format("Throttle should enforce a delay to respect bandwidth limit. Expected at least %d ms, but got %d ms.", expectedDelayMillis, durationMillis),
                durationMillis >= expectedDelayMillis - 1 // Allow margin for minor timing discrepancy
        );
    }
}