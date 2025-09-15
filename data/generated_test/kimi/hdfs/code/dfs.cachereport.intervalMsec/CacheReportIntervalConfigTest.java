package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class CacheReportIntervalConfigTest {

    @Mock
    private BPServiceActor bpServiceActor;

    private Configuration conf;
    private DNConf dnConf;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration(false);
        dnConf = new DNConf(conf);
    }

    @Test
    public void testCacheReportIntervalMsecDefaultValue() {
        // Given: No explicit configuration set, should use default
        long expectedDefault = DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT;

        // When: Get value directly from Configuration
        long actualValue = conf.getLong(
                DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
                DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT);

        // Then: Value matches default from DFSConfigKeys
        assertEquals(expectedDefault, actualValue);
    }

    @Test
    public void testCacheReportIntervalMsecCustomValue() {
        // Given: Custom configuration value
        long customValue = 5000L;
        conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, customValue);

        // When: Get value directly from Configuration
        long actualValue = conf.getLong(
                DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
                DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT);

        // Then: Value matches custom configuration
        assertEquals(customValue, actualValue);
    }

    @Test
    public void testConfigValueMatchesPropertiesFileLoader() {
        // Given: Load expected value from properties file (simulated)
        Properties props = new Properties();
        props.setProperty("dfs.cachereport.intervalMsec", "10000");
        conf.setLong(DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 10000L);

        // When: Get value via Configuration API
        long configValue = conf.getLong(
                DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
                DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT);

        // Then: Compare with value loaded from properties
        String propValueStr = props.getProperty("dfs.cachereport.intervalMsec");
        long propValue = Long.parseLong(propValueStr);
        assertEquals(propValue, configValue);
    }
}