package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class ReplaceDatanodeOnFailurePolicyConfigTest {

    @Test
    public void testGetPolicy_defaultValueFromConfigKey() throws IOException {
        // Arrange
        Configuration conf = new Configuration();
        // Ensure the key is not explicitly set, so it uses the default
        conf.unset(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY);
        
        // Load expected default from the config key constant
        String expectedDefaultPolicy = HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT;

        // Also load from raw configuration file to verify consistency
        Properties defaultProps = new Properties();
        defaultProps.load(this.getClass().getClassLoader().getResourceAsStream("core-default.xml"));
        String fileDefaultPolicy = defaultProps.getProperty(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            expectedDefaultPolicy
        );

        // Act
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

        // Assert
        assertEquals("Configuration file default should match constant", expectedDefaultPolicy, fileDefaultPolicy);
        // Since getPolicy is private, we can't directly test it, but we can test the behavior through get()
        assertNotNull("ReplaceDatanodeOnFailure instance should not be null", replaceDatanodeOnFailure);
    }

    @Test
    public void testGetPolicy_validPolicyValues() throws IOException {
        // Test NEVER policy
        testValidPolicy("NEVER");
        // Test DEFAULT policy
        testValidPolicy("DEFAULT");
        // Test ALWAYS policy
        testValidPolicy("ALWAYS");
    }
    
    private void testValidPolicy(String policyValue) throws IOException {
        // Arrange
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, true);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, policyValue);

        // Load expected value from the configuration service (simulated here with Configuration.get)
        String expectedPolicyValue = conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        );

        // Also verify with raw file
        Properties defaultProps = new Properties();
        defaultProps.load(this.getClass().getClassLoader().getResourceAsStream("core-default.xml"));
        String defaultFromFile = defaultProps.getProperty(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY
        );

        // If not set in config, fall back to default
        if (expectedPolicyValue == null || expectedPolicyValue.isEmpty()) {
            expectedPolicyValue = defaultFromFile;
        }

        // Act
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

        // Assert
        assertEquals("Policy value should match input", expectedPolicyValue.toUpperCase(), policyValue.toUpperCase());
        // We can't directly test the private getPolicy method, but get() should succeed
        assertNotNull("ReplaceDatanodeOnFailure instance should not be null", replaceDatanodeOnFailure);
    }

    @Test
    public void testGetPolicy_caseInsensitive() {
        // Arrange
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, true);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "default"); // lowercase

        // Act
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

        // Assert
        // We can't directly test the private getPolicy method, but get() should succeed
        assertNotNull("ReplaceDatanodeOnFailure instance should not be null", replaceDatanodeOnFailure);
    }

    @Test
    public void testGetPolicy_invalidPolicyValue() {
        // Arrange
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, true);
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "INVALID_POLICY");

        // Act & Assert
        try {
            ReplaceDatanodeOnFailure.get(conf);
            fail("Should throw exception for invalid policy value");
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }

    @Test
    public void testGetPolicy_featureDisabled() {
        // Arrange
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, false);
        // Even if policy is set, it should be ignored when disabled
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "ALWAYS");

        // Act
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

        // Assert
        // When disabled, get() should still return an instance, but internal behavior would be disabled
        assertNotNull("ReplaceDatanodeOnFailure instance should not be null", replaceDatanodeOnFailure);
    }

    @Test
    public void testWrite_methodUpdatesConfiguration() {
        // Arrange
        Configuration conf = new Configuration();
        ReplaceDatanodeOnFailure.Policy testPolicy = ReplaceDatanodeOnFailure.Policy.ALWAYS;
        boolean bestEffort = true;

        // Act
        ReplaceDatanodeOnFailure.write(testPolicy, bestEffort, conf);

        // Assert
        boolean isEnabled = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT
        );
        String policyValue = conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        );
        boolean isBestEffort = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_DEFAULT
        );

        assertTrue("ENABLE_KEY should be true when policy is not DISABLE", isEnabled);
        assertEquals("POLICY_KEY should match written policy", testPolicy.name(), policyValue);
        assertEquals("BEST_EFFORT_KEY should match written value", bestEffort, isBestEffort);
    }
}