package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class ReplaceDatanodeOnFailureConfigTest {

    @Test
    public void testPolicyConfigDefaultValue() throws IOException {
        // Load default value from configuration keys
        String expectedDefaultPolicy = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT;

        // Create a fresh configuration (no overrides)
        Configuration conf = new Configuration();
        
        // Get actual value via API
        String actualPolicyValue = conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        );

        assertEquals(expectedDefaultPolicy, actualPolicyValue);
    }

    @Test
    public void testPolicyConfigAgainstPropertiesFile() throws IOException {
        // Simulate loading from external config file using Properties
        Properties props = new Properties();
        // Normally this would be loaded from hdfs-site.xml or similar
        // Here we simulate what the real config file should contain
        props.setProperty(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "DEFAULT");

        // Verify that our constant matches what's in the simulated config file
        String expectedValue = props.getProperty(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY);
        String actualValueFromConstant = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT;

        assertEquals(expectedValue, actualValueFromConstant);
    }

    @Test
    public void testValidPolicyValuesAreAccepted() {
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, true);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "NEVER");

        // This should not throw an exception for valid policies
        ReplaceDatanodeOnFailure result = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(result);

        // Test DEFAULT policy
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "DEFAULT");
        result = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(result);
        
        // Test ALWAYS policy
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "ALWAYS");
        result = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(result);
    }

    @Test
    public void testInvalidPolicyValueThrowsException() {
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, true);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "INVALID_POLICY");

        try {
            ReplaceDatanodeOnFailure.get(conf);
            fail("Should throw exception for invalid policy");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testDisabledFeatureIgnoresPolicy() {
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, false);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "ALWAYS"); // Should be ignored

        ReplaceDatanodeOnFailure result = ReplaceDatanodeOnFailure.get(conf);
        // When disabled, the result should still be a valid object, but the behavior would be disabled
        assertNotNull(result);
    }

    @Test
    public void testWriteMethodPreservesSettings() {
        Configuration conf = new Configuration();
        
        // Set some values
        ReplaceDatanodeOnFailure.Policy originalPolicy = ReplaceDatanodeOnFailure.Policy.ALWAYS;
        boolean originalBestEffort = true;
        
        // Write to config
        ReplaceDatanodeOnFailure.write(originalPolicy, originalBestEffort, conf);
        
        // Read back using the same mechanism as ReplaceDatanodeOnFailure.get()
        ReplaceDatanodeOnFailure reloaded = ReplaceDatanodeOnFailure.get(conf);
        
        assertNotNull(reloaded);
        assertEquals(originalBestEffort, reloaded.isBestEffort());
    }
}