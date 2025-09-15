package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ImageServlet.class, DataTransferThrottler.class})
public class ImageTransferBandwidthConfigTest {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
    }

    @Test
    // testNoThrottlerWhenZeroBandwidth
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testNoThrottlerWhenZeroBandwidth() throws Exception {
        // 1. Use the HDFS 2.8.5 API to correctly obtain configuration values
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L);
        long configuredBandwidth = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 
                                               DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
        
        // 2. Prepare the test conditions - bandwidth is set to zero
        assertEquals("Configuration value should be 0", 0L, configuredBandwidth);
        
        // 3. Test code - invoke ImageServlet.getThrottler with the prepared configuration
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // 4. Code after testing - assert that the returned DataTransferThrottler instance is null
        assertNull("Throttler should be null when bandwidth is 0", throttler);
    }

    @Test
    public void testThrottlerCreatedWhenPositiveBandwidth() throws Exception {
        // 1. Use the HDFS 2.8.5 API to correctly obtain configuration values
        long testBandwidth = 1024 * 1024; // 1 MB/s
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);
        long configuredBandwidth = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 
                                               DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
        
        // 2. Prepare the test conditions - bandwidth is set to positive value
        assertEquals("Configuration value should match test bandwidth", testBandwidth, configuredBandwidth);
        
        // Mock the DataTransferThrottler constructor
        DataTransferThrottler mockThrottler = mock(DataTransferThrottler.class);
        whenNew(DataTransferThrottler.class).withArguments(testBandwidth).thenReturn(mockThrottler);
        
        // 3. Test code - invoke ImageServlet.getThrottler with the prepared configuration
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // 4. Code after testing - assert that the returned DataTransferThrottler instance is not null
        assertNotNull("Throttler should be created when bandwidth > 0", throttler);
        assertSame("Should return the mocked throttler", mockThrottler, throttler);
    }

    @Test
    public void testDefaultBandwidthValue() {
        // 1. Use the HDFS 2.8.5 API to correctly obtain configuration values
        long defaultValue = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 
                                        DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
        
        // 2. Prepare the test conditions - no explicit configuration set
        // 3. Test code - check default value
        // 4. Code after testing - assert default value is 0
        assertEquals("Default bandwidth should be 0", 0L, defaultValue);
        assertEquals("Configuration default value should be 0", 0L, DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
    }

    @Test
    public void testThrottlerBandwidthCalculation() {
        // 1. Use the HDFS 2.8.5 API to correctly obtain configuration values
        long configuredBandwidth = 2048 * 1024; // 2 MB/s
        
        // 2. Prepare the test conditions - create throttler with specific bandwidth
        // 3. Test code - create throttler and get bandwidth
        DataTransferThrottler throttler = new DataTransferThrottler(configuredBandwidth);
        long actualBandwidth = throttler.getBandwidth();
        
        // 4. Code after testing - assert correct bandwidth calculation
        assertEquals("Throttler should report correct bandwidth", configuredBandwidth, actualBandwidth);
    }

    @Test
    public void testThrottlerThrottleMethodWithZeroBytes() {
        // 1. Use the HDFS 2.8.5 API to correctly obtain configuration values
        long configuredBandwidth = 1024;
        
        // 2. Prepare the test conditions - create throttler and canceler
        DataTransferThrottler throttler = new DataTransferThrottler(configuredBandwidth);
        
        // 3. Test code - call throttle with zero bytes
        // 4. Code after testing - should not throw exception
        throttler.throttle(0);
    }

    @Test
    public void testThrottlerThrottleMethodWithNegativeBytes() {
        // 1. Use the HDFS 2.8.5 API to correctly obtain configuration values
        long configuredBandwidth = 1024;
        
        // 2. Prepare the test conditions - create throttler and canceler
        DataTransferThrottler throttler = new DataTransferThrottler(configuredBandwidth);
        
        // 3. Test code - call throttle with negative bytes
        // 4. Code after testing - should not throw exception
        throttler.throttle(-100);
    }
}