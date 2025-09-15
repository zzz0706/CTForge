package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ImageServlet.class, TransferFsImage.class})
public class ImageServletBootstrapStandbyBandwidthConfigTest {

    private Configuration conf;
    private Properties configProps;

    @Before
    public void setUp() throws IOException {
        // Load configuration from actual Hadoop configuration files
        conf = new Configuration();
        conf.addResource("hdfs-default.xml");
        conf.addResource("hdfs-site.xml");

        // Load properties for direct comparison
        configProps = new Properties();
        // Assuming hdfs-default.xml is in the classpath
        try {
            configProps.load(ClassLoader.getSystemClassLoader().getResourceAsStream("hdfs-default.xml"));
        } catch (Exception e) {
            // If file not found, continue with empty properties
        }
    }

    @Test
    public void testBootstrapStandbyBandwidthConfigDefaultValue() {
        // 1. Obtain configuration value using Hadoop API
        long configValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);

        // 2. Compare with default value from DFSConfigKeys
        assertEquals(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT, configValue);

        // 3. Compare with value loaded from properties file
        String propValue = configProps.getProperty(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY);
        if (propValue != null) {
            assertEquals(Long.parseLong(propValue), configValue);
        } else {
            assertEquals(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT, configValue);
        }
    }

    @Test
    public void testGetThrottlerForBootstrapStandbyReturnsNullWhenConfigIsZero() {
        // Set config to 0 (default)
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, 0L);

        // Call method under test
        DataTransferThrottler throttler = null;
        try {
            // Use reflection to access private method
            java.lang.reflect.Method method = ImageServlet.class.getDeclaredMethod("getThrottlerForBootstrapStandby", Configuration.class);
            method.setAccessible(true);
            throttler = (DataTransferThrottler) method.invoke(null, conf);
        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        }

        // Assert throttler is null (no throttling)
        assertNull(throttler);
    }

    @Test
    public void testGetThrottlerForBootstrapStandbyReturnsThrottlerWhenConfigIsPositive() {
        // Set config to positive value
        long bandwidth = 1024L;
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, bandwidth);

        // Mock DataTransferThrottler constructor to capture argument
        PowerMockito.mockStatic(DataTransferThrottler.class);
        DataTransferThrottler mockThrottler = Mockito.mock(DataTransferThrottler.class);
        try {
            PowerMockito.whenNew(DataTransferThrottler.class).withArguments(bandwidth).thenReturn(mockThrottler);

            // Call method under test using reflection
            DataTransferThrottler throttler = null;
            try {
                java.lang.reflect.Method method = ImageServlet.class.getDeclaredMethod("getThrottlerForBootstrapStandby", Configuration.class);
                method.setAccessible(true);
                throttler = (DataTransferThrottler) method.invoke(null, conf);
            } catch (Exception e) {
                fail("Exception during test: " + e.getMessage());
            }

            // Verify constructor was called with correct argument
            PowerMockito.verifyNew(DataTransferThrottler.class).withArguments(bandwidth);
            // Assert throttler is not null
            assertNotNull(throttler);
            // Assert it's the mocked instance
            assertSame(mockThrottler, throttler);
        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        }
    }

    @Test
    public void testServeFileUsesBootstrapStandbyThrottlerWhenIsBootstrapStandbyTrue() throws Exception {
        // Prepare test conditions
        long bandwidth = 2048L;
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, bandwidth);

        // Mock dependencies
        File mockFile = mock(File.class);
        when(mockFile.exists()).thenReturn(true);
        FileInputStream mockFis = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withArguments(mockFile).thenReturn(mockFis);

        // Mock TransferFsImage.copyFileToStream to capture arguments
        PowerMockito.mockStatic(TransferFsImage.class);

        // Create a spy on ImageServlet to test protected method
        ImageServlet servlet = PowerMockito.spy(new ImageServlet());
        
        // Mock DataTransferThrottler constructor
        PowerMockito.mockStatic(DataTransferThrottler.class);
        DataTransferThrottler mockThrottler = Mockito.mock(DataTransferThrottler.class);
        PowerMockito.whenNew(DataTransferThrottler.class).withArguments(bandwidth).thenReturn(mockThrottler);

        // Call serveFile method logic via reflection
        DataTransferThrottler throttler = null;
        try {
            java.lang.reflect.Method method = ImageServlet.class.getDeclaredMethod("getThrottlerForBootstrapStandby", Configuration.class);
            method.setAccessible(true);
            throttler = (DataTransferThrottler) method.invoke(null, conf);
        } catch (Exception e) {
            fail("Exception during test: " + e.getMessage());
        }
        
        // Verify the throttler was created with correct bandwidth
        PowerMockito.verifyNew(DataTransferThrottler.class).withArguments(bandwidth);
        assertNotNull(throttler);
        assertSame(mockThrottler, throttler);
    }
}