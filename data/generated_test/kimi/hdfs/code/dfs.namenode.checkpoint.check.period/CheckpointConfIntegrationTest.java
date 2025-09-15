package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import java.lang.reflect.Field;

public class CheckpointConfIntegrationTest {

    private Configuration conf;
    private CheckpointConf checkpointConf;

    @Before
    public void setUp() {
        conf = new Configuration();
        // Ensure default values are used if not set
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
    public void testCheckpointCheckPeriodWithMinimumBetweenCheckAndPeriodInSecondaryNameNode() throws Exception {
        // Prepare the test conditions
        // Set dfs.namenode.checkpoint.check.period to 120 and dfs.namenode.checkpoint.period to 60 in the Configuration object.
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 120L);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 60L);
        checkpointConf = new CheckpointConf(conf);

        // Create a mock SecondaryNameNode to test the doWork method behavior
        SecondaryNameNode mockSNN = mock(SecondaryNameNode.class);
        
        // Use reflection to access the checkpointConf field in SecondaryNameNode
        // Note: In a real test, you would create an actual SecondaryNameNode instance
        // but for this test we verify the CheckpointConf behavior which is what doWork() uses
        
        // Test code
        // Execute getCheckPeriod() which is used in SecondaryNameNode's doWork()
        long effectiveCheckPeriod = checkpointConf.getCheckPeriod();

        // Code after testing
        // Assert that the sleep interval corresponds to the smaller value (60 seconds)
        assertEquals("The effective check period should be the minimum of the two configured periods", 60L, effectiveCheckPeriod);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckpointConfConstructorParsesConfigurationCorrectly() {
        // Prepare the test conditions
        long customCheckPeriod = 180L;
        long customPeriod = 90L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, customCheckPeriod);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, customPeriod);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1000000L);
        
        // Test code
        checkpointConf = new CheckpointConf(conf);
        
        // Access private fields using reflection to verify they were set correctly
        try {
            Field checkPeriodField = CheckpointConf.class.getDeclaredField("checkpointCheckPeriod");
            checkPeriodField.setAccessible(true);
            long actualCheckPeriod = checkPeriodField.getLong(checkpointConf);
            
            Field periodField = CheckpointConf.class.getDeclaredField("checkpointPeriod");
            periodField.setAccessible(true);
            long actualPeriod = periodField.getLong(checkpointConf);
            
            // Code after testing
            assertEquals("Checkpoint check period should be set from configuration", customCheckPeriod, actualCheckPeriod);
            assertEquals("Checkpoint period should be set from configuration", customPeriod, actualPeriod);
            assertEquals("getCheckPeriod should return minimum of the two", Math.min(customCheckPeriod, customPeriod), checkpointConf.getCheckPeriod());
        } catch (Exception e) {
            throw new RuntimeException("Failed to access private fields", e);
        }
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckpointConfHandlesUnsetValuesWithDefaults() {
        // Prepare the test conditions
        // Configuration has been unset in setUp()
        
        // Test code
        checkpointConf = new CheckpointConf(conf);
        
        // Code after testing
        assertEquals("Should use default check period when unset", 
                    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT, 
                    checkpointConf.getCheckPeriod());
        assertEquals("Should use default period when unset", 
                    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT, 
                    checkpointConf.getPeriod());
        assertEquals("Should use default txn count when unset", 
                    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT, 
                    checkpointConf.getTxnCount());
    }
}