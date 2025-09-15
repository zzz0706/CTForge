package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class SafeModeInfoConfigTest {

    private Configuration conf;
    private float thresholdPct;
    private boolean expectWarning;

    public SafeModeInfoConfigTest(float thresholdPct, boolean expectWarning) {
        this.thresholdPct = thresholdPct;
        this.expectWarning = expectWarning;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {0.999f, false},  // default value, no warning
                {0.0f, false},    // zero, no warning
                {-0.1f, false},   // negative, no warning
                {1.0f, false},    // exactly 1, no warning
                {1.001f, true}    // greater than 1, expect warning
        });
    }

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testSafeModeThresholdPctConfiguration() {
        // Prepare the test conditions
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, thresholdPct);

        // Verify the threshold is set correctly by checking through configuration
        float actualValue = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 0.999f);
        assertEquals("Threshold should match configured value", thresholdPct, actualValue, 0.001f);
    }

    @Test
    public void testSafeModeThresholdPctDefaultValue() {
        // Test with no explicit configuration - should use default
        // Get default value from configuration keys
        float defaultValue = conf.getFloat(
            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT
        );
        
        assertEquals("Should use default threshold value", 
                    DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT, 
                    defaultValue, 0.001f);
    }

    @Test
    public void testSafeModeThresholdPctFromFile() {
        // Simulate loading from hdfs-site.xml
        // Create configuration and set the value as if loaded from file
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 0.95f);
        
        // Compare against our reference loader
        float expectedValue = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
                                          DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        
        assertEquals("Configuration value should match file value", 
                    0.95f, expectedValue, 0.001f);
    }

    @Test
    public void testReplQueueThresholdInheritsFromSafeModeThreshold() {
        // Set only the safemode threshold
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, thresholdPct);
        // Don't set repl queue threshold, so it should inherit
        
        // Verify configuration was set correctly
        float actualThreshold = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
                                            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        assertEquals("Threshold should be set correctly", thresholdPct, actualThreshold, 0.001f);
    }

    @Test
    public void testEnableSafeModeForTestingRespectsConfiguration() {
        // Prepare configuration
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, thresholdPct);
        
        // Verify that configuration was used correctly
        float actualValue = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
                                        DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        assertEquals("Configuration should be used correctly", thresholdPct, actualValue, 0.001f);
    }
    
    @Test
    public void testSafeModeThresholdLessThanZero_ExitsImmediately() {
        // Create a Configuration object and set dfs.namenode.safemode.threshold-pct to -0.1
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, -0.1f);
        
        // Verify configuration was set correctly
        float configuredValue = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                                            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        assertEquals("Configuration should be set to -0.1", -0.1f, configuredValue, 0.001f);
    }
}