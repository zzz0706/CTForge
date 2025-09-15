package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ImageServlet.class, TransferFsImage.class, FileInputStream.class})
public class ImageServletBootstrapStandbyBandwidthConfigTest {

    @Mock
    private Configuration mockConf;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    // Helper method to access private method using reflection
    private DataTransferThrottler invokeGetThrottlerForBootstrapStandby(Configuration conf) throws Exception {
        Method method = ImageServlet.class.getDeclaredMethod("getThrottlerForBootstrapStandby", Configuration.class);
        method.setAccessible(true);
        return (DataTransferThrottler) method.invoke(null, conf);
    }

    @Test
    public void testBootstrapStandbyBandwidthConfigDefaultValue() throws Exception {
        // Given: Load default configuration from Hadoop's configuration system
        Configuration conf = new Configuration();
        long defaultValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT
        );

        // When: Get the value using the same method as in the target code
        long actualValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT
        );

        // Then: Assert that the value matches the expected default (0)
        assertEquals("Default value should be 0", 0L, actualValue);
        assertEquals("Default value should match constant", 
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT, 
                defaultValue);
    }

    @Test
    public void testBootstrapStandbyBandwidthConfigFromFile() throws Exception {
        // Given: Load configuration from external file (simulated)
        Properties props = new Properties();
        // Simulate loading from hdfs-default.xml or similar
        // In real scenario, this would be loaded from actual config files
        props.setProperty("dfs.image.transfer-bootstrap-standby.bandwidthPerSec", "1048576"); // 1MB/s
        
        Configuration conf = new Configuration();
        // Set the property to simulate file-based configuration
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, 1048576L);

        // When: Get the configured value
        long configuredValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT
        );

        // Then: Verify it matches what we set
        assertEquals("Configured value should match file value", 1048576L, configuredValue);
    }

    @Test
    public void testGetThrottlerForBootstrapStandbyReturnsNullWhenZero() throws Exception {
        // Given: Configuration with zero bandwidth (default)
        when(mockConf.getLong(
                eq(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY),
                anyLong())
        ).thenReturn(0L);

        // When: Call the method under test via reflection
        DataTransferThrottler throttler = invokeGetThrottlerForBootstrapStandby(mockConf);

        // Then: Should return null (no throttling)
        assertNull("Throttler should be null when bandwidth is 0", throttler);
    }

    @Test
    public void testGetThrottlerForBootstrapStandbyCreatesThrottlerWhenPositive() throws Exception {
        // Given: Configuration with positive bandwidth
        long bandwidth = 1048576L; // 1MB/s
        when(mockConf.getLong(
                eq(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY),
                anyLong())
        ).thenReturn(bandwidth);

        // Mock DataTransferThrottler constructor
        DataTransferThrottler mockThrottler = mock(DataTransferThrottler.class);
        PowerMockito.whenNew(DataTransferThrottler.class)
                .withArguments(bandwidth)
                .thenReturn(mockThrottler);

        // When: Call the method under test via reflection
        DataTransferThrottler throttler = invokeGetThrottlerForBootstrapStandby(mockConf);

        // Then: Should create and return a throttler
        assertNotNull("Throttler should not be null when bandwidth > 0", throttler);
        PowerMockito.verifyNew(DataTransferThrottler.class).withArguments(bandwidth);
    }

    @Test
    public void testServeFileUsesBootstrapStandbyThrottler() throws Exception {
        // Given: Mock setup for ImageServlet.serveFile method
        long bandwidth = 2097152L; // 2MB/s
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, bandwidth);
        
        File mockFile = mock(File.class);
        when(mockFile.exists()).thenReturn(true);
        FileInputStream mockFis = mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class)
                .withAnyArguments()
                .thenReturn(mockFis);

        // Mock TransferFsImage.copyFileToStream to capture arguments
        PowerMockito.mockStatic(TransferFsImage.class);

        // When: Simulate the serveFile call with bootstrap standby flag
        // Test the getThrottlerForBootstrapStandby method directly via reflection
        DataTransferThrottler throttler = invokeGetThrottlerForBootstrapStandby(conf);
        
        // Verify that throttler was created with correct bandwidth
        assertNotNull("Throttler should be created", throttler);
        
        // Use reflection or argument capturing to verify throttler creation
        ArgumentCaptor<Long> bandwidthCaptor = ArgumentCaptor.forClass(Long.class);
        
        // Since we can't easily capture constructor args, we'll verify through behavior
        PowerMockito.whenNew(DataTransferThrottler.class)
                .withArguments(bandwidthCaptor.capture())
                .thenReturn(mock(DataTransferThrottler.class));
        
        invokeGetThrottlerForBootstrapStandby(conf);
        
        assertEquals("Bandwidth should match configured value", 
                bandwidth, bandwidthCaptor.getValue().longValue());
    }

    @Test
    public void testConfigValueConsistencyWithHadoopDefaults() throws Exception {
        // Given: Two ways of accessing the same configuration
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();
        
        // When: Get the same configuration value through both methods
        long value1 = conf1.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT
        );
        
        String stringValue = conf2.get(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY);
        long value2 = stringValue != null ? Long.parseLong(stringValue) : 
                     DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT;
        
        // Then: Values should be consistent
        assertEquals("Configuration values should be consistent", value1, value2);
        assertEquals("Default value should be 0", 0L, value1);
    }
}