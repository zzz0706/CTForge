package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ImageServlet.class, DataTransferThrottler.class})
public class ImageTransferBandwidthConfigTest {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
    }

    @Test
    public void testBandwidthConfigDefaultValue() {
        // Verify default value from configuration service matches expected
        long defaultValue = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
        assertEquals("Default bandwidth should be 0", 0L, defaultValue);
        
        // Verify the default value from DFSConfigKeys is also 0
        assertEquals("Configuration default value should be 0", 0L, DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
    }

    @Test
    public void testThrottlerCreationWhenBandwidthIsZero() throws Exception {
        // Given
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L);
        
        // When
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // Then
        assertNull("Throttler should be null when bandwidth is 0", throttler);
    }

    @Test
    public void testThrottlerCreationWhenBandwidthIsPositive() throws Exception {
        // Given
        long testBandwidth = 1024 * 1024; // 1 MB/s
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);
        
        DataTransferThrottler mockThrottler = mock(DataTransferThrottler.class);
        PowerMockito.whenNew(DataTransferThrottler.class)
                   .withArguments(testBandwidth)
                   .thenReturn(mockThrottler);
        
        // When
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        
        // Then
        assertNotNull("Throttler should be created when bandwidth > 0", throttler);
        assertSame("Should return the mocked throttler", mockThrottler, throttler);
    }

    @Test
    public void testThrottlerBandwidthCalculation() {
        // Given
        long configuredBandwidth = 2048 * 1024; // 2 MB/s
        
        // When
        DataTransferThrottler throttler = new DataTransferThrottler(configuredBandwidth);
        long actualBandwidth = throttler.getBandwidth();
        
        // Then
        assertEquals("Throttler should report correct bandwidth", configuredBandwidth, actualBandwidth);
    }

    @Test
    public void testNoThrottlingWhenBytesAreZeroOrNegative() {
        // Given
        long configuredBandwidth = 1024;
        DataTransferThrottler throttler = new DataTransferThrottler(configuredBandwidth);
        
        // When/Then - Should not throw exception or cause issues
        throttler.throttle(0);
        throttler.throttle(-100);
        // Test passes if no exceptions thrown
    }
}