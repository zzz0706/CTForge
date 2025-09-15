package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DfsClientConfBlockWriteRetriesTest {

    private Configuration conf;
    private int expectedRetries;
    private Properties configProperties;

    public DfsClientConfBlockWriteRetriesTest(int configuredRetries) {
        this.expectedRetries = configuredRetries;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {0},   // No retries
                {1},   // One retry
                {3},   // Default value
                {5},   // Custom value
                {10}   // Higher value
        });
    }

    @Before
    public void setUp() {
        conf = new Configuration(false);
        if (expectedRetries != 3) { // 3 is the default, so only set if different
            conf.setInt(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, expectedRetries);
        }
        
        // Load reference values from properties
        configProperties = new Properties();
        // In a real test, this would load from actual config files
        configProperties.setProperty(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, String.valueOf(expectedRetries));
    }

    @Test
    public void testNumBlockWriteRetryConfiguration() {
        // Test that DfsClientConf correctly reads and stores the configuration
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        
        // Verify the configuration is correctly propagated
        assertEquals(expectedRetries, dfsClientConf.getNumBlockWriteRetry());
        
        // Verify against reference loader
        int propertyValue = Integer.parseInt(configProperties.getProperty(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, "3"));
        assertEquals(propertyValue, dfsClientConf.getNumBlockWriteRetry());
    }

    @Test
    public void testDefaultRetryValueWhenNotConfigured() {
        // Test default value when configuration is not explicitly set
        Configuration defaultConf = new Configuration(false);
        DfsClientConf dfsClientConf = new DfsClientConf(defaultConf);
        
        // Should use default value of 3
        assertEquals(HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT, dfsClientConf.getNumBlockWriteRetry());
        
        // Verify against reference loader with default - use default value directly
        int propertyValue = Integer.parseInt(configProperties.getProperty(
                HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, 
                String.valueOf(HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT)));
        // For this specific test, we should always expect the default value when no config is set
        assertEquals(HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT, dfsClientConf.getNumBlockWriteRetry());
    }
}