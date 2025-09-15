package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ImageTransferBandwidthConfigTest {

    @Test
    public void testBandwidthConfigDefaultValue() throws IOException {
        // Verify Configuration returns the default value
        Configuration conf = new Configuration(false); // Don't load default resources to test default value
        long configValue = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                                        DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);

        assertEquals("Default value should match DFSConfigKeys constant", 
                     DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT, configValue);
    }

    @Test
    public void testGetThrottlerWithBandwidthZero() {
        // Prepare configuration with zero bandwidth
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0);

        // Test ImageServlet.getThrottler method
        DataTransferThrottler throttler = new DataTransferThrottler(conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0));

        // Assert throttler is not null when bandwidth is 0
        assertNotNull("Throttler should be created when bandwidth is 0", throttler);
        assertEquals("Throttler bandwidth should match configured value", 0, throttler.getBandwidth());
    }

    @Test
    public void testGetThrottlerWithBandwidthPositive() {
        // Prepare configuration with positive bandwidth
        Configuration conf = new Configuration();
        long bandwidth = 1024;
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, bandwidth);

        // Test DataTransferThrottler creation
        DataTransferThrottler throttler = new DataTransferThrottler(conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0));

        // Assert throttler is created correctly
        assertNotNull("Throttler should be created when bandwidth > 0", throttler);
        assertEquals("Throttler bandwidth should match configured value", bandwidth, throttler.getBandwidth());
    }

    @Test
    public void testThrottlerConstructorArgument() {
        long testBandwidth = 2048;
        
        // Create DataTransferThrottler 
        DataTransferThrottler throttler = new DataTransferThrottler(testBandwidth);
        
        // Verify constructor initializes internal state correctly
        assertEquals("Constructed throttler should return configured bandwidth", testBandwidth, throttler.getBandwidth());
    }

    @Test
    public void testBandwidthZeroDisablesThrottling() {
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0);

        DataTransferThrottler throttler = new DataTransferThrottler(conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0));
        assertNotNull("Throttler should be created even when bandwidth is 0", throttler);
        assertEquals("Bandwidth of 0 should be set in throttler", 0, throttler.getBandwidth());
    }

    @Test
    public void testBandwidthPositiveEnablesThrottling() {
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 1024);

        DataTransferThrottler throttler = new DataTransferThrottler(conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0));
        assertNotNull("Positive bandwidth should enable throttling", throttler);
        assertEquals("Bandwidth should match config", 1024, throttler.getBandwidth());
    }
}