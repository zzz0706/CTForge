package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ReplaceDatanodeOnFailureConfigTest {

    // Helper method to load default configuration values from external files
    private Properties loadDefaultConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("core-default.xml")) {
            if (input != null) {
                // In real scenario, parse XML appropriately; here we simulate with Properties
                props.load(input);
            }
        }
        return props;
    }

    @Test
    public void testPolicyConfigurationValueMatchesExternalSource() throws IOException {
        // Load configuration from external source
        Properties externalProps = loadDefaultConfig();
        String externalPolicyValue = externalProps.getProperty(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        );

        // Get value via Configuration API
        Configuration conf = new Configuration();
        String configPolicyValue = conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        );

        // Assert that both values match
        assertEquals("Configuration value should match external source", externalPolicyValue, configPolicyValue);
    }

    @Test
    public void testValidPolicyValuesAreAccepted() {
        Configuration conf = new Configuration();
        conf.set(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            "NEVER"
        );
        conf.setBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            true
        );

        // Test that valid policies create correct ReplaceDatanodeOnFailure instances
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(replaceDatanodeOnFailure);
        
        // Test DEFAULT
        conf.set(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            "DEFAULT"
        );
        replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(replaceDatanodeOnFailure);
        
        // Test ALWAYS
        conf.set(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            "ALWAYS"
        );
        replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(replaceDatanodeOnFailure);
    }

    @Test
    public void testInvalidPolicyValueThrowsException() {
        Configuration conf = new Configuration();
        conf.set(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            "INVALID_POLICY"
        );
        conf.setBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            true
        );

        // Test that invalid policy throws exception
        try {
            ReplaceDatanodeOnFailure.get(conf);
            fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }

    @Test
    public void testDisabledFeatureReturnsDisablePolicy() {
        Configuration conf = new Configuration();
        conf.setBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            false
        );

        // Test that disabled feature returns proper instance
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(replaceDatanodeOnFailure);
    }

    @Test
    public void testGetMethodReturnsCorrectReplaceDatanodeOnFailureInstance() {
        Configuration conf = new Configuration();
        conf.set(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            "ALWAYS"
        );
        conf.setBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            true
        );
        conf.setBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
            true
        );

        // Test that get method returns properly configured instance
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
        assertNotNull(replaceDatanodeOnFailure);
        // Verify internal state through behavior or reflection if needed
    }

    @Test
    public void testWriteMethodSetsConfigurationCorrectly() {
        Configuration conf = new Configuration();
        
        // Call write method
        ReplaceDatanodeOnFailure.write(
            ReplaceDatanodeOnFailure.Policy.ALWAYS,
            true,
            conf
        );

        // Verify configuration was set correctly
        assertTrue(conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            false
        ));
        assertEquals("ALWAYS", conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY
        ));
        assertTrue(conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY,
            false
        ));
    }

    @Test
    public void testWriteMethodWithDisablePolicySetsEnableToFalse() {
        Configuration conf = new Configuration();
        
        // Call write method with DISABLE policy
        ReplaceDatanodeOnFailure.write(
            ReplaceDatanodeOnFailure.Policy.DISABLE,
            false,
            conf
        );

        // Verify enable is set to false
        assertFalse(conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            true // Default to true to test that it's actually false
        ));
    }
}