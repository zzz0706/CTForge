package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestImageServlet {
    
    @Test
    // Test code to verify getThrottler method functionality
    // 1. Use the hdfs 2.8.5 API to retrieve the configuration value for "dfs.image.transfer.bandwidthPerSec".
    // 2. Set up a Configuration object with valid bandwidth for testing.
    // 3. Call getThrottler method and verify it returns a valid throttler.
    // 4. Ensure the throttler's bandwidth matches the configuration.
    public void test_getThrottler_withValidConfiguration() {
        // Step 1: Prepare the test conditions by setting a valid bandwidth in the Configuration object
        Configuration conf = new Configuration();
        long testBandwidth = 1048576L; // 1MB per second for testing
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);

        // Step 2: Invoke the getThrottler method from ImageServlet
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Validate the returned throttler instance
        assertNotNull("Expected DataTransferThrottler instance, but got null.", throttler);
        assertEquals("Throttler bandwidth does not match the configured value.", 
                     testBandwidth, throttler.getBandwidth());
    }
    
    @Test
    // Test code to verify the throttle method of DataTransferThrottler
    // 1. Use a throttler instance and simulate a transfer beyond the allowed rate.
    // 2. Measure if the throttler enforces the expected delay to regulate the bandwidth.
    // 3. Verify correct behavior during throttling using HDFS APIs.
    public void test_throttle_withRateLimiting() throws InterruptedException {
        // Step 1: Prepare the test DataTransferThrottler with a defined bandwidth
        long testBandwidth = 1024L; // 1KB per second for testing
        DataTransferThrottler throttler = new DataTransferThrottler(testBandwidth);

        // Step 2: Simulate data transfer that requires throttling
        long bytesToSend = 2048L; // Simulating the transfer of 2KB
        long startTime = System.currentTimeMillis();
        throttler.throttle(bytesToSend, null); // Enforce throttling for the data transfer
        long duration = System.currentTimeMillis() - startTime;

        // Step 3: Calculate the expected throttling delay (in milliseconds)
        long expectedMinDelayMs = (bytesToSend * 1000) / testBandwidth;

        // Step 4: Verify that throttling delay respects the configured bandwidth
        assertEquals("Throttle delay does not match expected timing (approximation).", 
                     expectedMinDelayMs, duration, 100); // Allowing slight deviation (±100ms)
    }
    
    @Test
    // Test code to verify getThrottler handles invalid or zero bandwidth values correctly
    // 1. Ensure no throttler instance is created for zero or missing configuration.
    // 2. Use the hdfs 2.8.5 APIs to retrieve and validate behaviors.
    public void test_getThrottler_withZeroBandwidthConfiguration() {
        // Step 1: Set up configuration with zero bandwidth
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L); // Invalid bandwidth

        // Step 2: Call getThrottler method and verify the returned value
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Ensure no throttler is created for invalid configuration
        assertEquals("Expected null throttler for zero bandwidth configuration.", null, throttler);
    }
}