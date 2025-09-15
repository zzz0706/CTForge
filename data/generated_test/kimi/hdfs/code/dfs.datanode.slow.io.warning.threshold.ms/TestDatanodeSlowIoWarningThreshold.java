package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

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
    public void testSlowIoWarningThresholdConfigValue() {
        // 1. Obtain configuration value using HDFS API
        long expectedThreshold = conf.getLong(
                DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
                DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

        // 2. Verify that DNConf correctly parses the configuration
        assertEquals(expectedThreshold, dnConf.datanodeSlowIoWarningThresholdMs);
    }

    @Test
    public void testDefaultSlowIoWarningThresholdValue() {
        // 1. Test with default configuration (no explicit setting)
        long defaultThreshold = DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT;
        dnConf = new DNConf(new Configuration());

        // 2. Verify default value is used
        assertEquals(defaultThreshold, dnConf.datanodeSlowIoWarningThresholdMs);
    }

    @Test
    public void testCustomSlowIoWarningThresholdValue() {
        // 1. Setup configuration with custom threshold
        long customThreshold = 500L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, customThreshold);
        dnConf = new DNConf(conf);

        // 2. Verify that DNConf correctly parses the custom configuration
        assertEquals(customThreshold, dnConf.datanodeSlowIoWarningThresholdMs);
    }

    @Test
    public void testSlowIoWarningThresholdZeroValue() {
        // 1. Setup configuration with zero threshold (disabled)
        long zeroThreshold = 0L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, zeroThreshold);
        dnConf = new DNConf(conf);

        // 2. Verify that DNConf correctly parses the zero configuration
        assertEquals(zeroThreshold, dnConf.datanodeSlowIoWarningThresholdMs);
    }

    @Test
    public void testSlowIoWarningThresholdNegativeValue() {
        // 1. Setup configuration with negative threshold 
        long negativeThreshold = -100L;
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY, negativeThreshold);
        dnConf = new DNConf(conf);

        // 2. Verify that DNConf accepts negative values as configured (no validation)
        assertEquals(negativeThreshold, dnConf.datanodeSlowIoWarningThresholdMs);
    }
}