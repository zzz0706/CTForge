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
    }

    @Test
    public void testCheckpointCheckPeriodDefaultValue() {
        // Prepare test conditions: Do not set the config value, so default is used
        checkpointConf = new CheckpointConf(conf);

        // Test: Check that the default value is correctly loaded from DFSConfigKeys
        long expectedDefault = DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT; // 60
        long actualValue = checkpointConf.getCheckPeriod(); // This should return the min of check period and period

        // Since we didn't set dfs.namenode.checkpoint.period, it will use its default (3600)
        // So the minimum should be 60 (dfs.namenode.checkpoint.check.period)
        assertEquals(expectedDefault, actualValue);
    }

    @Test
    public void testCheckpointCheckPeriodCustomValue() {
        // Prepare test conditions: Set a custom value for the config
        long customCheckPeriod = 120L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, customCheckPeriod);
        checkpointConf = new CheckpointConf(conf);

        // Test: Ensure the custom value is respected when both periods are larger
        long customCheckpointPeriod = 300L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, customCheckpointPeriod);

        // Re-initialize to pick up new checkpointPeriod
        checkpointConf = new CheckpointConf(conf);

        // The minimum of 120 and 300 should be 120
        long expectedMin = Math.min(customCheckPeriod, customCheckpointPeriod);
        long actualValue = checkpointConf.getCheckPeriod();

        assertEquals(expectedMin, actualValue);
    }

    @Test
    public void testCheckpointCheckPeriodWithSmallerCheckpointPeriod() {
        // Prepare test conditions: Make checkpoint period smaller than check period
        long checkPeriod = 200L;
        long checkpointPeriod = 100L;

        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, checkPeriod);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, checkpointPeriod);
        checkpointConf = new CheckpointConf(conf);

        // Test: getCheckPeriod should return the smaller of the two
        long expectedMin = Math.min(checkPeriod, checkpointPeriod);
        long actualValue = checkpointConf.getCheckPeriod();

        assertEquals(expectedMin, actualValue);
    }

    @Test
    public void testGetCheckPeriodReturnsMinimumValue() {
        // Test that getCheckPeriod returns the minimum of checkpoint period and check period
        long checkPeriod = 150L;
        long checkpointPeriod = 90L;
        
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, checkPeriod);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, checkpointPeriod);
        checkpointConf = new CheckpointConf(conf);
        
        long expected = Math.min(checkPeriod, checkpointPeriod);
        long actual = checkpointConf.getCheckPeriod();
        
        assertEquals(expected, actual);
    }

    @Test
    public void testGetCheckPeriodWithDefaultValues() {
        // Test with default values for both configuration parameters
        checkpointConf = new CheckpointConf(conf);
        
        long checkPeriodDefault = DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT;
        long checkpointPeriodDefault = DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT;
        
        long expected = Math.min(checkPeriodDefault, checkpointPeriodDefault);
        long actual = checkpointConf.getCheckPeriod();
        
        assertEquals(expected, actual);
    }
}