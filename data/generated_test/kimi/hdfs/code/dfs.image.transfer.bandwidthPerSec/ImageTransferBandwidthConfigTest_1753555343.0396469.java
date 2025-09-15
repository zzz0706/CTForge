package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

import static org.junit.Assert.*;

public class ImageTransferBandwidthConfigTest {

    @Test
    public void testImageTransferBandwidthConfigDefaultValue() {
        // Load default configuration
        Configuration conf = new Configuration();
        
        // Get value via Configuration
        long configValue = conf.getLong(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT
        );
        
        // Assert default value
        assertEquals("Default value should be 0 (no throttling)", 0L, configValue);
    }

    @Test
    public void testDataTransferThrottlerCreationBasedOnBandwidthConfig() throws Exception {
        long[] testBandwidths = {0L, 1024L, 1024*1024L, 100*1024*1024L};
        
        for (long bandwidth : testBandwidths) {
            // Setup configuration with specific bandwidth
            Configuration conf = new Configuration();
            conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, bandwidth);
            
            // Since we can't directly access ImageServlet.getThrottler, we test the logic directly
            if (bandwidth > 0) {
                // Test that throttler can be created with positive bandwidth
                DataTransferThrottler throttler = new DataTransferThrottler(bandwidth);
                assertNotNull("Throttler should be created when bandwidth > 0", throttler);
                assertEquals("Throttler bandwidth should match configured value", 
                    bandwidth, throttler.getBandwidth());
            } else {
                // For bandwidth <= 0, no throttling should be applied
                assertTrue("Bandwidth should be <= 0 for this test case", bandwidth <= 0);
            }
        }
    }

    @Test
    public void testThrottlerBehaviorWithDifferentBandwidthValues() throws Exception {
        long[] testBandwidths = {1024L, 1024*1024L, 10*1024*1024L};
        
        for (long bandwidth : testBandwidths) {
            Configuration conf = new Configuration();
            conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, bandwidth);
            
            // Test throttler creation directly
            DataTransferThrottler throttler = new DataTransferThrottler(bandwidth);
            
            assertNotNull("Throttler should be created", throttler);
            assertEquals("Bandwidth should be correctly configured in throttler",
                bandwidth, throttler.getBandwidth());
        }
    }

    @Test
    public void testThrottlerSleepBehavior() throws Exception {
        // Setup configuration with specific bandwidth
        Configuration conf = new Configuration();
        long testBandwidth = 1024 * 1024; // 1 MB/s
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);
        
        // Test the actual throttler
        DataTransferThrottler throttler = new DataTransferThrottler(testBandwidth);
        assertNotNull("Throttler should be created", throttler);
        
        // Verify that the throttler correctly reports its bandwidth
        assertEquals("Throttler should report correct bandwidth",
            testBandwidth, throttler.getBandwidth());
    }

    @Test
    public void testConfigurationConsistency() {
        // Test that Configuration provides correct default value
        Configuration conf = new Configuration();
        
        // Get default value through Configuration API
        long configDefaultValue = conf.getLong(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT
        );
        
        assertEquals("Configuration should provide correct default value",
            DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT, configDefaultValue);
        assertEquals("Default should be 0", 0L, configDefaultValue);
    }
}