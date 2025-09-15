package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestDfsClientMmapCacheSize {

    @Test
    public void testDfsClientMmapCacheSizeConfigValidity() {
        // Step 1: Load the Hadoop Configuration
        Configuration conf = new Configuration();

        // Step 2: Read the specific configuration value
        int mmapCacheSize = conf.getInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY,
                HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT);

        // Step 3: Validate the configuration constraints
        // Constraint 1: The value of dfs.client.mmap.cache.size must be > 0
        Assert.assertTrue("The configuration 'dfs.client.mmap.cache.size' must be > 0",
                mmapCacheSize > 0);

        // Step 4: Print validation successful message (Optional for detailed test logs)
        System.out.println("Configuration 'dfs.client.mmap.cache.size' is valid: "
                + mmapCacheSize);
    }
}