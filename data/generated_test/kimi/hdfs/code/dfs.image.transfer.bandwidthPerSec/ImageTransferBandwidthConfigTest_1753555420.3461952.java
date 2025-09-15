package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

import static org.junit.Assert.*;

public class ImageTransferBandwidthConfigTest {

    @Test
    // Test that DataTransferThrottler correctly enforces the configured bandwidth limit
    // 1. Retrieve the configuration value for dfs.image.transfer.bandwidthPerSec using Configuration.getLong()
    // 2. Create a DataTransferThrottler instance using the configured value
    // 3. Measure time before calling throttle()
    // 4. Call throttle() with a byte count that would exceed the bandwidth limit within the period
    // 5. Measure time after throttle() returns
    // 6. Assert that the elapsed time reflects the expected throttling delay
    public void testThrottlerAppliesRateLimitingCorrectly() throws Exception {
        // Setup configuration with specific bandwidth (1 MB/s)
        Configuration conf = new Configuration();
        long testBandwidth = 1024 * 1024; // 1 MB/s
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);
        
        // Retrieve the configuration value
        long configValue = conf.getLong(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT
        );
        
        // Create a DataTransferThrottler instance using ImageServlet helper method
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // Verify throttler was created correctly
        assertNotNull("Throttler should be created when bandwidth > 0", throttler);
        assertEquals("Throttler bandwidth should match configured value", 
            configValue, throttler.getBandwidth());
        
        // Measure time before calling throttle()
        long startTime = System.nanoTime();
        
        // Call throttle() with a byte count that would exceed the bandwidth limit
        // Transfer 2MB which should take at least 2 seconds at 1MB/s rate
        throttler.throttle(2 * 1024 * 1024); // 2MB
        
        // Measure time after throttle() returns
        long endTime = System.nanoTime();
        
        // Calculate elapsed time in milliseconds
        long elapsedMs = (endTime - startTime) / 1_000_000;
        
        // Assert that the elapsed time reflects the expected throttling delay
        // Should be at least 1 second (1000ms) for 2MB at 1MB/s, with some tolerance
        assertTrue("Throttling should enforce bandwidth limit - took " + elapsedMs + "ms", 
            elapsedMs >= 1000);
    }

    @Test
    // Test that ImageServlet.getThrottler correctly handles configuration values
    // 1. Use Configuration API to set different bandwidth values
    // 2. Call ImageServlet.getThrottler with the configuration
    // 3. Verify the returned throttler behavior
    public void testImageServletGetThrottlerWithConfig() {
        // Test with positive bandwidth value
        Configuration conf = new Configuration();
        long testBandwidth = 1024 * 1024; // 1 MB/s
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);
        
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        assertNotNull("Throttler should be created when bandwidth > 0", throttler);
        assertEquals("Throttler should have correct bandwidth", 
            testBandwidth, throttler.getBandwidth());
        
        // Test with zero bandwidth (should return null)
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L);
        throttler = ImageServlet.getThrottler(conf);
        assertNull("Throttler should be null when bandwidth is 0", throttler);
        
        // Test with negative bandwidth (should return null)
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, -1L);
        throttler = ImageServlet.getThrottler(conf);
        assertNull("Throttler should be null when bandwidth is negative", throttler);
    }

    @Test
    // Test that DataTransferThrottler correctly uses the configuration through ImageServlet
    // 1. Set up configuration with specific bandwidth
    // 2. Create throttler via ImageServlet
    // 3. Verify throttler behavior matches configuration
    public void testDataTransferThrottlerCreationViaImageServlet() {
        long[] testBandwidths = {0L, 1024L, 1024*1024L, 100*1024*1024L};
        
        for (long bandwidth : testBandwidths) {
            // Setup configuration with specific bandwidth
            Configuration conf = new Configuration();
            conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, bandwidth);
            
            // Use ImageServlet to create throttler
            DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
            
            if (bandwidth > 0) {
                // Test that throttler can be created with positive bandwidth
                assertNotNull("Throttler should be created when bandwidth > 0", throttler);
                assertEquals("Throttler bandwidth should match configured value", 
                    bandwidth, throttler.getBandwidth());
            } else {
                // For bandwidth <= 0, no throttling should be applied
                assertNull("Throttler should be null when bandwidth <= 0", throttler);
            }
        }
    }

    @Test
    // Test throttler behavior with small transfer respecting bandwidth limits
    // 1. Configure bandwidth limit
    // 2. Create throttler via ImageServlet
    // 3. Test throttle with small data transfer
    public void testThrottlerWithSmallTransfer() throws Exception {
        // Setup configuration
        Configuration conf = new Configuration();
        long testBandwidth = 1024 * 1024; // 1 MB/s
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);
        
        // Create throttler
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        assertNotNull("Throttler should be created", throttler);
        
        // Test with small transfer that should take predictable time
        // 512KB at 1MB/s should take approximately 500ms
        long startTime = System.nanoTime();
        throttler.throttle(512 * 1024); // 512KB
        long endTime = System.nanoTime();
        
        long elapsedMs = (endTime - startTime) / 1_000_000;
        // Should take approximately 500ms for 512KB at 1MB/s rate, with some tolerance
        assertTrue("Small transfer should respect bandwidth limit - took " + elapsedMs + "ms", 
            elapsedMs >= 400 && elapsedMs <= 600);
    }

    @Test
    // Test configuration default value handling through ImageServlet
    // 1. Create configuration without setting bandwidth
    // 2. Verify ImageServlet.getThrottler returns null (no throttling)
    public void testImageTransferBandwidthConfigDefaultValueViaImageServlet() {
        // Load default configuration
        Configuration conf = new Configuration();
        
        // Get value via Configuration
        long configValue = conf.getLong(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT
        );
        
        // Assert default value
        assertEquals("Default value should be 0 (no throttling)", 0L, configValue);
        
        // Verify ImageServlet.getThrottler behavior with default config
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        assertNull("Throttler should be null with default configuration", throttler);
    }
}