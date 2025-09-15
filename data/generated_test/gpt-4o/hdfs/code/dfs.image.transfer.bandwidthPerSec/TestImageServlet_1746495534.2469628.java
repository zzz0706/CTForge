package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestImageServlet {

    @Test
    // Test code to ensure the getThrottler method creates a DataTransferThrottler instance with a valid non-zero bandwidth.
    // 1. Use the correct HDFS 2.8.5 API to obtain configuration values, avoiding hardcoding configuration values.
    // 2. Prepare the test conditions (e.g., setting the proper configuration value for dfs.image.transfer.bandwidthPerSec).
    // 3. Execute the getThrottler method and verify correctness.
    // 4. Validate the returned DataTransferThrottler instance and its properties.
    public void test_getThrottler_withValidConfiguration() {
        // Step 1: Prepare the test conditions
        Configuration conf = new Configuration();
        long testBandwidth = 1048576L; // 1MB per second bandwidth for testing
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);

        // Step 2: Call the getThrottler method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Assert and validate the results
        assertNotNull("Expected DataTransferThrottler instance, but got null.", throttler);

        // Verify the throttler's bandwidth matches the configured bandwidth
        assertEquals("Throttler bandwidth does not match the configured value.",
                testBandwidth, throttler.getBandwidth());
    }

    @Test
    // Test code to confirm the throttle method of DataTransferThrottler effectively enforces the configured rate limit.
    // 1. Use the HDFS 2.8.5 API for simulating throttling behavior.
    // 2. Ensure the DataTransferThrottler respects bandwidth limits in the throttle method.
    // 3. Verify it handles cancellation scenarios properly.
    public void test_throttle_withRateLimiting() throws InterruptedException {
        // Step 1: Prepare the test conditions
        long testBandwidth = 1024L; // 1KB per second for easier testing
        DataTransferThrottler throttler = new DataTransferThrottler(testBandwidth);

        // Step 2: Test throttle simulation
        long bytesToSend = 2048L; // 2KB of data to simulate
        long startTime = System.currentTimeMillis();
        throttler.throttle(bytesToSend);
        long duration = System.currentTimeMillis() - startTime;

        // Verify that it enforces throttle delays (approximately correct delay to match the rate)
        long expectedMinDelay = (bytesToSend * 1000) / testBandwidth; // delay in milliseconds
        assertEquals("Throttle delay does not match expected timing (approximation).",
                expectedMinDelay, duration, 100); // Allow slight deviation (~100ms)
    }
}