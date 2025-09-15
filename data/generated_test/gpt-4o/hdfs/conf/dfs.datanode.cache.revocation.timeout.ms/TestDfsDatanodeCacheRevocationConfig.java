package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDfsDatanodeCacheRevocationConfig {

    /**
     * Test to validate the configuration value of 'dfs.datanode.cache.revocation.timeout.ms'
     * and ensure it conforms to constraints and dependencies.
     */
    @Test
    public void testRevocationTimeoutConfig() {
        Configuration config = new Configuration();

        // Read the value of dfs.datanode.cache.revocation.timeout.ms
        long revocationTimeoutMs = config.getLong(
                DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS,
                DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT);

        // Validate that the value is non-negative
        assertTrue("The value of dfs.datanode.cache.revocation.timeout.ms must be non-negative.",
                revocationTimeoutMs >= 0);

        // Read the value of dfs.datanode.cache.revocation.polling.ms
        long revocationPollingMs = config.getLong(
                "dfs.datanode.cache.revocation.polling.ms",
                5000L // Using a reasonable fallback default (assumed from context)
        );

        // Validate dependency: revocationPollingMs should be at most revocationTimeoutMs / 2
        long minRevocationPollingMs = revocationTimeoutMs / 2;
        assertTrue("The value of dfs.datanode.cache.revocation.polling.ms must not be more than half " +
                        "of the value of dfs.datanode.cache.revocation.timeout.ms. Expected value <= " +
                        minRevocationPollingMs,
                revocationPollingMs <= minRevocationPollingMs);
    }
}