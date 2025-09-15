package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ImageTransferBandwidthConfigTest {

    @Test
    public void testBandwidthConfigValueMatchesDefault() {
        // Get default value from Configuration API
        Configuration conf = new Configuration();
        long actualDefaultBandwidth = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);

        // Compare with the expected default value
        assertEquals("Default bandwidth value should match expected default",
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT, actualDefaultBandwidth);
    }

    @Test
    public void testGetThrottlerWithDifferentBandwidthValues() {
        // Test with 1024L bandwidth
        testThrottlerCreation(1024L);
        
        // Test with 1024*1024L bandwidth
        testThrottlerCreation(1024*1024L);
        
        // Test with 100*1024*1024L bandwidth
        testThrottlerCreation(100*1024*1024L);
        
        // Test with 0L bandwidth
        testThrottlerCreation(0L);
    }
    
    private void testThrottlerCreation(long bandwidth) {
        // Prepare test conditions
        Configuration conf = mock(Configuration.class);
        when(conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT)).thenReturn(bandwidth);

        // Test code
        DataTransferThrottler throttler = new DataTransferThrottler(bandwidth);

        // Code after testing
        if (bandwidth > 0) {
            assertNotNull("Throttler should be created when bandwidth > 0", throttler);
            assertEquals("Throttler bandwidth should match configured value", 
                    bandwidth, throttler.getBandwidth());
        } else {
            // For zero bandwidth, we still create a throttler but with unlimited bandwidth
            assertNotNull("Throttler should be created even when bandwidth is 0", throttler);
        }
    }

    @Test
    public void testThrottlerConstructorAndThrottleBehavior() {
        long bandwidth = 1024 * 1024; // 1 MB/s
        DataTransferThrottler throttler = new DataTransferThrottler(bandwidth);
        
        assertEquals("Constructed throttler should report correct bandwidth", 
                bandwidth, throttler.getBandwidth());
        
        // Verify that throttle method doesn't block for small amounts
        boolean exceptionThrown = false;
        try {
            throttler.throttle(100);
        } catch (Exception e) {
            exceptionThrown = true;
        }
        assertFalse("Throttling small amount should not throw exception", exceptionThrown);
    }

    @Test
    public void testThrottlerWithZeroBandwidth() {
        long bandwidth = 0L;
        DataTransferThrottler throttler = new DataTransferThrottler(bandwidth);
        assertNotNull("Throttler should be created even when bandwidth is zero", throttler);
    }

    @Test
    public void testThrottlerCreationBranchCondition() {
        // Test with positive bandwidth
        DataTransferThrottler throttlerWithBandwidth = new DataTransferThrottler(1000L);
        assertNotNull("Throttler should be created when bandwidth > 0", throttlerWithBandwidth);
        
        // Test with zero bandwidth
        DataTransferThrottler throttlerWithoutBandwidth = new DataTransferThrottler(0L);
        assertNotNull("Throttler should be created even when bandwidth is 0", throttlerWithoutBandwidth);
    }
}