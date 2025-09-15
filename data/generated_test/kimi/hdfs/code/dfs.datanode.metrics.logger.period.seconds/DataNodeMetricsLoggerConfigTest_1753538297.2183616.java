package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class DataNodeMetricsLoggerConfigTest {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
    }

    @Test
    public void testMetricsLoggerPeriod_DefaultValue() throws Exception {
        // Given - No explicit configuration set
        
        // When
        long actualValue = conf.getLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, 
                                       DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);
        
        // Then
        assertEquals("Default value should match configuration default", 
                    DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT, actualValue);
    }

    @Test
    public void testMetricsLoggerUsesConfiguredValue() throws Exception {
        // Given
        long configuredValue = 120L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, configuredValue);
        
        // When
        long actualValue = conf.getLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
                                       DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);
        
        // Then
        assertEquals("Configured value should be used", configuredValue, actualValue);
    }

    @Test
    public void testMetricsLoggerPeriod_ZeroValue() throws Exception {
        // Given
        conf.setLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, 0);
        
        // When
        long actualValue = conf.getLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
                                       DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);
        
        // Then
        assertEquals("Zero value should be preserved", 0L, actualValue);
    }

    @Test
    public void testMetricsLoggerPeriod_NegativeValue() throws Exception {
        // Given
        conf.setLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, -1);
        
        // When
        long actualValue = conf.getLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
                                       DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);
        
        // Then
        assertEquals("Negative value should be preserved", -1L, actualValue);
    }

    @Test
    public void testMetricsLoggerPeriod_PositiveValue() throws Exception {
        // Given
        long periodSeconds = 300L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY, periodSeconds);
        
        // When
        long actualValue = conf.getLong(DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_KEY,
                                       DFSConfigKeys.DFS_DATANODE_METRICS_LOGGER_PERIOD_SECONDS_DEFAULT);
        
        // Then
        assertEquals("Positive value should be used", periodSeconds, actualValue);
    }
}