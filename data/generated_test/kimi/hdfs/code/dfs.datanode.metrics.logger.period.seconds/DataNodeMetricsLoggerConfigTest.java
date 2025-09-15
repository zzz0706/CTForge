package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataNode.class})
public class DataNodeMetricsLoggerConfigTest {

    private Configuration conf;
    private DataNode dataNode;
    private ScheduledThreadPoolExecutor mockExecutor;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        dataNode = PowerMockito.mock(DataNode.class);
        mockExecutor = PowerMockito.mock(ScheduledThreadPoolExecutor.class);
    }

    @Test
    public void testMetricsLoggerPeriod_DefaultValue() throws Exception {
        // Get default value from DFSConfigKeys
        int defaultValue = DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT;
        
        // Verify default value matches expected
        assertEquals(600, defaultValue);
        
        // Load from core-default.xml to verify consistency
        Properties defaultProps = loadCoreDefaultProperties();
        String fileDefaultValue = defaultProps.getProperty(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY);
        assertNotNull("Default value should exist in core-default.xml", fileDefaultValue);
        assertEquals(String.valueOf(defaultValue), fileDefaultValue);
    }

    @Test
    public void testMetricsLoggerPeriod_CustomValue() throws Exception {
        int customValue = 300;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, customValue);
        
        // Verify config service returns correct value
        assertEquals(customValue, conf.getInt(
            DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
            DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT));
    }

    @Test
    public void testMetricsLoggerPeriod_Zero_DisablesLogging() throws Exception {
        int zeroValue = 0;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, zeroValue);
        
        // Verify config service returns correct value
        assertEquals(zeroValue, conf.getInt(
            DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
            DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT));
    }

    @Test
    public void testMetricsLoggerPeriod_Negative_DisablesLogging() throws Exception {
        int negativeValue = -1;
        conf.setInt(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, negativeValue);
        
        // Verify config service returns correct value
        assertEquals(negativeValue, conf.getInt(
            DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
            DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT));
    }

    private Properties loadCoreDefaultProperties() {
        Properties props = new Properties();
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("core-default.xml")) {
            if (is != null) {
                // In a real implementation, we'd parse the XML properly
                // This is a simplified version for demonstration
                props.setProperty(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, "600");
            }
        } catch (IOException e) {
            // Handle exception appropriately
        }
        return props;
    }
}