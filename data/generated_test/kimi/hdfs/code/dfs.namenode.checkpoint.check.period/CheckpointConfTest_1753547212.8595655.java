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
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
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
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
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
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
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

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckpointCheckPeriodWithMinimumBetweenCheckAndPeriodInSecondaryNameNode() {
        // Prepare the test conditions
        // Set dfs.namenode.checkpoint.check.period to 120 and dfs.namenode.checkpoint.period to 60 in the Configuration object.
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 120L);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 60L);
        checkpointConf = new CheckpointConf(conf);

        // Test code
        // Execute getCheckPeriod() which is used in SecondaryNameNode's doWork()
        long effectiveCheckPeriod = checkpointConf.getCheckPeriod();

        // Code after testing
        // Assert that the sleep interval corresponds to the smaller value (60 seconds)
        assertEquals("The effective check period should be the minimum of the two configured periods", 60L, effectiveCheckPeriod);
    }
}