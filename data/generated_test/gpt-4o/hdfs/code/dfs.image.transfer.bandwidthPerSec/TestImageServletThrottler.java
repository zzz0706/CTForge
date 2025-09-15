package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestImageServletThrottler {

    @Test
    // Test code to verify the functionality of getThrottler with a valid bandwidth configuration
    // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    //    - Set dfs.image.transfer.bandwidthPerSec in the Configuration object to 1048576 using the proper API.
    // 3. Verify that getThrottler returns a valid DataTransferThrottler object with the configured bandwidth.
    // 4. Ensure the test executes without throwing exceptions and validates the expected behavior.
    public void test_getThrottler_withValidConfiguration() {
        // Step 1: Prepare configuration with a valid non-zero bandwidth
        Configuration conf = new Configuration();
        long expectedBandwidth = 1048576L; // 1MB per second for testing
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, expectedBandwidth);

        // Step 2: Call the getThrottler method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Assert the output is as expected
        assertNotNull("Expected DataTransferThrottler instance, but got null.", throttler);
        assertEquals("Throttler bandwidth does not match the configured value.",
                     expectedBandwidth, throttler.getBandwidth());
    }

    @Test
    // Test code to verify the behavior of getThrottler when bandwidth is set to 0
    // 1. Use the hdfs 2.8.5 API correctly to configure dfs.image.transfer.bandwidthPerSec to zero.
    // 2. Verify that the getThrottler method returns null for zero or invalid configurations.
    // 3. Ensure the test cases validate this edge case correctly.
    public void test_getThrottler_withZeroBandwidthConfiguration() {
        // Step 1: Prepare configuration with zero bandwidth
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L); // Invalid bandwidth configuration

        // Step 2: Call the getThrottler method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Assert no throttler is created for zero bandwidth
        assertEquals("Expected null throttler for zero bandwidth configuration.", null, throttler);
    }

    @Test
    // Test code to verify the throttle method of DataTransferThrottler with enforced rate-limiting
    // 1. Use the hdfs 2.8.5 API correctly to initialize a DataTransferThrottler with a defined bandwidth (e.g., 1KB/s).
    // 2. Simulate data transfer exceeding the bandwidth and enforce throttling.
    // 3. Measure and validate the actual delay caused due to throttling.
    public void test_throttle_withRateLimiting() throws InterruptedException {
        // Step 1: Prepare a DataTransferThrottler instance with a defined bandwidth
        long throttleBandwidth = 1024L; // 1KB per second for the test case
        DataTransferThrottler throttler = new DataTransferThrottler(throttleBandwidth);

        // Step 2: Simulate data transfer beyond the rate limit
        long bytesToSend = 2048L; // Simulating transfer of 2KB data
        long startTime = System.currentTimeMillis();
        throttler.throttle(bytesToSend, null); // Trigger throttling for the given data size
        long duration = System.currentTimeMillis() - startTime;

        // Step 3: Calculate the expected delay in milliseconds
        long expectedDelayMs = (bytesToSend * 1000) / throttleBandwidth;

        // Step 4: Validate that the throttling enforces the expected delay
        assertEquals("Throttle delay does not match expected timing (within a slight margin).",
                     expectedDelayMs, duration, 100); // Allow slight deviation
    }
}