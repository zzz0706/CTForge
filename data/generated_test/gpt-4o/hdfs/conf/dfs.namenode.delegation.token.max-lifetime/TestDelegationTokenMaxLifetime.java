package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

public class TestDelegationTokenMaxLifetime {

    private Configuration conf;

    @Before
    public void setup() {
        // Initialize the configuration object before tests.
        conf = new Configuration();
    }

    @Test
    public void testDelegationTokenMaxLifetimeConfiguration() {
        // Test code for validating the "dfs.namenode.delegation.token.max-lifetime" configuration.

        // 1. Retrieve the configuration value using the HDFS API.
        long delegationTokenMaxLifetime = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT
        );

        // 2. Prepare the test conditions.
        // Check if the value satisfies the constraints.
        // The maximum lifetime should be a positive value in milliseconds.
        assertTrue(
            "The configuration value for dfs.namenode.delegation.token.max-lifetime must be positive.",
            delegationTokenMaxLifetime > 0
        );

        // Test code complete.
    }
}