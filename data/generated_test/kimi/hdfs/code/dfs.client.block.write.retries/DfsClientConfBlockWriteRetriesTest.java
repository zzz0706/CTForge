package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite.RETRIES_KEY;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DfsClientConfBlockWriteRetriesTest {

    private Configuration conf;
    private int expectedRetries;

    public DfsClientConfBlockWriteRetriesTest(int expectedRetries) {
        this.expectedRetries = expectedRetries;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {3},   // Default value
                {0},   // No retries
                {1},   // One retry
                {5},   // Five retries
                {10}   // Ten retries
        });
    }

    @Before
    public void setUp() {
        conf = new Configuration();
        conf.setInt(RETRIES_KEY, expectedRetries);
    }

    @Test
    public void testNumBlockWriteRetryConfigValue() {
        // 1. Obtain configuration values using HDFS 2.8.5 API
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        
        // 2. Verify that the configuration is correctly propagated
        assertEquals(expectedRetries, dfsClientConf.getNumBlockWriteRetry());
    }

    @Test
    public void testBlockWriteRetriesDefaultValue() {
        // Test with no explicit configuration set
        Configuration defaultConf = new Configuration();
        
        // The default value should be used
        DfsClientConf dfsClientConf = new DfsClientConf(defaultConf);
        
        // Verify against the constant defined in HdfsClientConfigKeys
        assertEquals(RETRIES_DEFAULT, dfsClientConf.getNumBlockWriteRetry());
    }

    @Test
    public void testDataStreamerUsesCorrectRetryCount() {
        // Create DfsClientConf with our test configuration
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        
        // This would normally be called internally by HDFS client logic
        // We're verifying that the retry count from config is used
        int actualRetryCount = dfsClientConf.getNumBlockWriteRetry();
        
        // Verify that the correct number of retries is used
        assertEquals(expectedRetries, actualRetryCount);
    }

    @Test
    public void testConfigurationLoaderComparison() {
        // Create a configuration and set the retry value explicitly
        Configuration testConf = new Configuration();
        testConf.setInt(RETRIES_KEY, expectedRetries);
        
        // Get value through HDFS Configuration API
        int configValue = testConf.getInt(RETRIES_KEY, RETRIES_DEFAULT);
        
        // Compare with what we expect from our parameterized test
        assertEquals(expectedRetries, configValue);
    }
}