package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestDfsClientConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_DfsClientConf_getNumBlockWriteRetry_custom_value() {
        // Step 1: Prepare test conditions
        // Create a Hadoop configuration instance and set a custom value for the configuration "dfs.client.block.write.retries".
        Configuration conf = new Configuration();
        conf.setInt("dfs.client.block.write.retries", 5);

        // Step 2: Instantiate DfsClientConf using the created configuration
        DfsClientConf dfsClientConf = new DfsClientConf(conf);

        // Step 3: Retrieve the configuration value using getNumBlockWriteRetry() and assert the expected result
        int numRetries = dfsClientConf.getNumBlockWriteRetry();
        assertEquals(5, numRetries);

        // Step 4: Code after testing
        // No additional teardown steps are required for this test
    }
}