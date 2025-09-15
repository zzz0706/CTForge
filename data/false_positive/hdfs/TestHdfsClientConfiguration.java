package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestHdfsClientConfiguration {

    @Test
    public void testDfsClientBlockWriteRetriesConfiguration() {
        // Step 1: Read the configuration value and verify its validity
        Configuration conf = new Configuration();

        // Retrieve the configuration value for `dfs.client.block.write.retries`
        int numBlockWriteRetries = conf.getInt(
                "dfs.client.block.write.retries",
                3 // Default value
        );

        // Step 2: Verify the constraints for the configuration value
        // Constraint: The number of retries should be a positive integer
        Assert.assertTrue(
            "Configuration value 'dfs.client.block.write.retries' must be a positive integer.",
            numBlockWriteRetries > 0
        );

    }
}