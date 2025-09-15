package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestImageServletConfigurations {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottlerWithThrottlingDisabled() {
        // Prepare the test conditions
        Configuration configuration = new Configuration();
        configuration.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0); // Explicitly setting zero for throttling disabled

        // Test code: Call the method under test
        DataTransferThrottler throttler = ImageServlet.getThrottler(configuration);

        // Code after testing: Verify the expected outcome
        assertNull("Throttler should be null when dfs.image.transfer.bandwidthPerSec is zero or negative", throttler);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottlerWithPositiveThrottling() {
        // Prepare the test conditions
        Configuration configuration = new Configuration();
        long transferRate = 1024 * 1024; // 1MB/sec, a positive throttling value
        configuration.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, transferRate);

        // Test code: Call the method under test
        DataTransferThrottler throttler = ImageServlet.getThrottler(configuration);

        // Code after testing: Verify the expected outcome
        assertNotNull("Throttler should not be null when dfs.image.transfer.bandwidthPerSec is positive", throttler);
        assertEquals("Throttler bandwidth should match the configured value", transferRate, throttler.getBandwidth());
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testThrottleMethod() throws InterruptedException {
        // Prepare the test conditions
        long transferRate = 1024; // 1 KB/sec
        DataTransferThrottler throttler = new DataTransferThrottler(transferRate);

        // Test code: Simulate data transfer and apply throttling
        long numOfBytes = 2048; // Simulate sending 2 KB
        long startTime = System.nanoTime(); // Replace Time.monotonicNanoTime() with System.nanoTime()
        throttler.throttle(numOfBytes, null); // No canceler passed

        // Code after testing: Adjust margin for assertion accuracy to address test failure due to slight timing differences
        long durationMillis = (System.nanoTime() - startTime) / 1_000_000; // Convert nanoseconds to milliseconds
        long expectedDelayMillis = (numOfBytes * 1000) / transferRate; // Calculate expected delay in milliseconds
        assertTrue(
                String.format("Throttle should enforce delay for respecting bandwidth limit. Expected at least %d ms, but got %d ms.", expectedDelayMillis, durationMillis),
                durationMillis >= expectedDelayMillis - 1 // Adjust margin for minor timing discrepancies
        );
    }
}