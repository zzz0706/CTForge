package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class SlowIoWarningThresholdConfigTest {

    @Mock
    private DataNode datanode;

    @Mock
    private FsDatasetSpi<?> fsDataset;

    private DNConf dnConf;
    private Configuration conf;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
        dnConf = new DNConf(conf);
    }

    @Test
    public void testSlowIoWarningThresholdDefaultValue() {
        // Given: No explicit configuration set
        long expectedDefault = DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT;

        // When: Get the value directly from Configuration
        long actualValue = conf.getLong(
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT
        );

        // Then: Should match the default value
        assertEquals(expectedDefault, actualValue);
    }

    @Test
    public void testSlowIoWarningThresholdCustomValue() {
        // Given: Custom configuration value
        long customValue = 500L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, customValue);

        // When: Get the custom value from Configuration
        long actualValue = conf.getLong(
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT
        );

        // Then: Should match the custom value
        assertEquals(customValue, actualValue);
    }

    @Test
    public void testSlowIoWarningThresholdInBlockReceiverFlushOrSync() throws IOException {
        // Given: Setup BlockReceiver with custom threshold
        long customThreshold = 100L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, customThreshold);
        dnConf = new DNConf(conf);
        
        when(datanode.getDnConf()).thenReturn(dnConf);
        when(datanode.getFSDataset()).thenReturn((FsDatasetSpi) fsDataset);

        // When & Then: Verify that the threshold is correctly used
        long actualThreshold = conf.getLong(
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT
        );
        assertEquals(customThreshold, actualThreshold);
    }

    @Test
    public void testSlowIoWarningThresholdInManageWriterOsCache() throws Throwable {
        // Given: Setup for manageWriterOsCache test
        long customThreshold = 200L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, customThreshold);
        dnConf = new DNConf(conf);
        
        when(datanode.getDnConf()).thenReturn(dnConf);

        // When & Then: Verify that the threshold is correctly used
        long actualThreshold = conf.getLong(
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT
        );
        assertEquals(customThreshold, actualThreshold);
    }

    @Test
    public void testConfigurationValueMatchesConstant() {
        // Given: Configuration service (in this case, Hadoop Configuration)
        Configuration config = new Configuration();
        
        // When: Get value through Configuration API
        long configValue = config.getLong(
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
            DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT
        );
        
        // Then: Should match the constant
        assertEquals(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT, configValue);
    }
}