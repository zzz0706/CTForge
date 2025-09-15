package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestDatanodeSlowIoWarningThreshold {

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
    public void testSlowIoWarningThresholdLoadedFromConfig() {
        // Given
        long expectedThreshold = conf.getLong(
                DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
                DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

        // When
        long actualThreshold = dnConf.datanodeSlowIoWarningThresholdMs;

        // Then
        assertEquals(expectedThreshold, actualThreshold);
    }

    @Test
    public void testSlowIoWarningThresholdWithCustomValue() {
        // Given
        long customThreshold = 500L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, customThreshold);
        DNConf customDnConf = new DNConf(conf);

        // When
        long actualThreshold = customDnConf.datanodeSlowIoWarningThresholdMs;

        // Then
        assertEquals(customThreshold, actualThreshold);
    }

    @Test
    public void testSlowIoWarningThresholdDefaultWhenNotSet() {
        // Given - no custom threshold set
        DNConf defaultDnConf = new DNConf(new Configuration());

        // When
        long actualThreshold = defaultDnConf.datanodeSlowIoWarningThresholdMs;

        // Then
        assertEquals(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT, actualThreshold);
    }
}