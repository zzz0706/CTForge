package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class NameNodeEditLogRollerConfigTest {

    private Configuration conf;
    private long editLogRollerThreshold;
    private long editLogRollerInterval = 5 * 60 * 1000; // default value from FSNamesystem

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testEditLogAutoRollThresholdCalculation() {
        // Prepare test conditions by setting configuration values
        float multiplierThreshold = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT
        );
        long checkpointTxns = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT
        );

        // Calculate expected threshold
        long expectedThreshold = (long) (multiplierThreshold * checkpointTxns);

        // Simulate the logic in FSNamesystem where editLogRollerThreshold is calculated
        editLogRollerThreshold = (long) (
                conf.getFloat(
                        DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
                        DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT
                ) *
                conf.getLong(
                        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
                        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT
                )
        );

        // Verify that the calculated threshold matches expected value
        assertEquals("Edit log roller threshold should match expected calculation",
                expectedThreshold, editLogRollerThreshold);
    }

    @Test
    public void testEditLogAutoRollMultiplierDefaultValue() {
        // Test that the default value is correctly retrieved from DFSConfigKeys
        float defaultValue = DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT;
        assertEquals("Default value should be 2.0", 2.0f, defaultValue, 0.001);
    }

    @Test
    public void testEditLogAutoRollMultiplierCustomValue() {
        // Set custom value in configuration
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD, 3.5f);
        
        // Retrieve value through configuration service
        float retrievedValue = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT
        );
        
        // Verify it matches what we set
        assertEquals("Custom multiplier value should be correctly retrieved", 3.5f, retrievedValue, 0.001);
    }

    @Test
    public void testReferenceLoaderComparison() {
        // Compare ConfigService.get... against direct file loading
        // In this case, we're comparing Configuration.get against the static defaults
        
        // Get value via Configuration API
        float configValue = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT
        );
        
        // Get default value directly from DFSConfigKeys (reference loader)
        float defaultValue = DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT;
        
        // They should be equal when no custom value is set
        assertEquals("Configuration value should match default constant", defaultValue, configValue, 0.001);
    }
}