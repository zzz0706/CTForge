package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

import static org.junit.Assert.*;

public class ImageTransferBandwidthConfigTest {

    @Test
    public void testNoThrottlerWhenNegativeBandwidth() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, -1L); // Set negative bandwidth
        
        // 3. Test code.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // 4. Code after testing.
        assertNull("Throttler should be null when bandwidth is negative", throttler);
    }

    @Test
    public void testThrottlerCreatedWhenPositiveBandwidth() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        long bandwidth = 1024L;
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, bandwidth);
        
        // 3. Test code.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // 4. Code after testing.
        assertNotNull("Throttler should be created when bandwidth is positive", throttler);
        assertEquals("Throttler bandwidth should match configured value", 
                bandwidth, throttler.getBandwidth());
    }

    @Test
    public void testNoThrottlerWhenZeroBandwidth() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L); // Set zero bandwidth
        
        // 3. Test code.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // 4. Code after testing.
        assertNull("Throttler should be null when bandwidth is zero", throttler);
    }

    @Test
    public void testThrottlerUsesDefaultValueWhenNotSet() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration(); // Do not set any bandwidth value
        
        // 3. Test code.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // 4. Code after testing.
        // According to the source code, the default value is 0, so no throttler should be created
        assertNull("Throttler should be null when using default value (0)", throttler);
    }

    @Test
    public void testBandwidthConfigValueMatchesDefault() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        
        // 3. Test code.
        long actualDefaultBandwidth = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
        
        // 4. Code after testing.
        assertEquals("Default bandwidth value should match expected default",
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT, actualDefaultBandwidth);
    }
}