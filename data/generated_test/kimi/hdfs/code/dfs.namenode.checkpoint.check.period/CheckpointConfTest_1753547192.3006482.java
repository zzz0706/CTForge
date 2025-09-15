package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckpointConfTest {

    private Configuration conf;
    private CheckpointConf checkpointConf;

    @Before
    public void setUp() {
        conf = new Configuration();
        // Ensure default value is used if not set
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY);
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY);
    }

    @Test
    public void testCheckpointCheckPeriodDefaultValue() {
        // Prepare the test conditions
        checkpointConf = new CheckpointConf(conf);

        // Test code
        long expectedDefault = DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT; // 60
        long actualValue = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT
        );

        // Code after testing
        assertEquals("Default value of dfs.namenode.checkpoint.check.period should be 60", expectedDefault, actualValue);
        assertEquals("CheckpointConf should return the default check period", expectedDefault, checkpointConf.getCheckPeriod());
    }

    @Test
    public void testCheckpointCheckPeriodCustomValue() {
        // Prepare the test conditions
        long customValue = 120L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, customValue);
        checkpointConf = new CheckpointConf(conf);

        // Test code
        long actualValue = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT
        );

        // Code after testing
        assertEquals("Custom value of dfs.namenode.checkpoint.check.period should be respected", customValue, actualValue);
        assertEquals("CheckpointConf should return the custom check period when less than checkpoint period", customValue, checkpointConf.getCheckPeriod());
    }

    @Test
    public void testGetCheckPeriodReturnsMinimum() {
        // Prepare the test conditions
        long checkPeriodValue = 300L;
        long checkPeriodCheckValue = 120L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, checkPeriodCheckValue);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, checkPeriodValue);
        checkpointConf = new CheckpointConf(conf);

        // Test code
        long expectedMin = Math.min(checkPeriodCheckValue, checkPeriodValue);

        // Code after testing
        assertEquals("getCheckPeriod should return the minimum of the two periods", expectedMin, checkpointConf.getCheckPeriod());
    }
}