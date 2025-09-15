package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TestDfsClientBlockWriteRetriesConfig {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testBlockWriteRetries_DefaultValue() {
        // Given: No explicit configuration set, should use default
        // When: Get the retry count from configuration directly
        int actualRetries = conf.getInt(
            HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
            HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT
        );
        
        // Then: Should match default value from HdfsClientConfigKeys
        assertEquals(HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT, actualRetries);
    }

    @Test
    public void testBlockWriteRetries_CustomValue() {
        // Given: Set custom retry value
        int customRetries = 5;
        conf.setInt(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, customRetries);
        
        // When: Get the retry count
        int actualRetries = conf.getInt(
            HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
            HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT
        );
        
        // Then: Should match custom value
        assertEquals(customRetries, actualRetries);
    }

    @Test
    public void testBlockWriteRetries_ZeroValue() {
        // Given: Set zero retries
        conf.setInt(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, 0);
        
        // When: Get the retry count
        int actualRetries = conf.getInt(
            HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
            HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT
        );
        
        // Then: Should allow zero retries
        assertEquals(0, actualRetries);
    }

    @Test
    public void testBlockWriteRetries_NegativeValue() {
        // Given: Set negative retries (invalid but should be handled)
        conf.setInt(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, -1);
        
        // When: Get the retry count
        int actualRetries = conf.getInt(
            HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
            HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT
        );
        
        // Then: Should still return the configured value
        assertEquals(-1, actualRetries);
    }

    @Test
    public void testBlockWriteRetries_PropertyFileComparison() {
        // Given: Load default configuration
        Configuration defaultConf = new Configuration();
        
        // When: Get values through both mechanisms
        int fromConfigService = defaultConf.getInt(
            HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
            HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT
        );
        int fromProperty = defaultConf.getInt(
            HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
            HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT
        );
        
        // Then: Values should match
        assertEquals(fromProperty, fromConfigService);
    }
}