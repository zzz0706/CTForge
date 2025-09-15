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
    // Test code
    // 1. Ensure the `dfs.image.transfer.bandwidthPerSec` configuration is correctly applied in the `Configuration` object using the appropriate HDFS API.
    // 2. Prepare the test conditions: properly configure a non-zero valid bandwidth.
    // 3. Call the `getThrottler` method and verify its behavior.
    // 4. Validate that the returned throttler has the expected bandwidth configuration.
    public void test_getThrottler_withValidConfiguration() {
        // Step 1: Setup configuration with valid bandwidth
        Configuration conf = new Configuration();
        long validBandwidth = 1048576L; // Example bandwidth: 1MB per second
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, validBandwidth);

        // Step 2: Call the `getThrottler` method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Validate the returned throttler instance
        assertNotNull("Expected a non-null DataTransferThrottler instance.", throttler);
        assertEquals("The throttler's bandwidth is not equal to the configured value.",
                     validBandwidth, throttler.getBandwidth());
    }

    @Test
    // Test code
    // 1. Use the `dfs.image.transfer.bandwidthPerSec` configuration with a zero value.
    // 2. Verify that the `getThrottler` method returns null.
    // 3. Ensure this test validates that no throttler is created for zero bandwidth.
    public void test_getThrottler_withZeroBandwidthConfiguration() {
        // Step 1: Setup configuration with zero bandwidth
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L);

        // Step 2: Call the `getThrottler` method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Validate the output
        assertEquals("Expected null when no throttler should be created.", null, throttler);
    }

    @Test
    // Test code
    // 1. Initialize a throttler with a specific bandwidth configuration using the API.
    // 2. Simulate data transfer beyond the configured rate limit and enforce throttling.
    // 3. Verify the delay caused by throttling.
    public void test_throttle_withRateLimiting() throws InterruptedException {
        // Step 1: Create a DataTransferThrottler with known bandwidth
        long throttleBandwidth = 1024L; // Example: 1KB per second
        DataTransferThrottler throttler = new DataTransferThrottler(throttleBandwidth);

        // Step 2: Simulate data transfer beyond the throttle rate
        long bytesToSend = 2048L; // Example: Sending data of size 2KB
        long startTime = System.currentTimeMillis();
        throttler.throttle(bytesToSend, null); // Invoke throttling
        long actualDuration = System.currentTimeMillis() - startTime;

        // Step 3: Compute expected delay
        long expectedDelayMs = (bytesToSend * 1000) / throttleBandwidth;

        // Step 4: Verify the actual delay approximately matches the expected value
        assertEquals("Throttling delay is not as expected (allowing small margin).",
                     expectedDelayMs, actualDuration, 100); // Allow slight timing variation
    }
}