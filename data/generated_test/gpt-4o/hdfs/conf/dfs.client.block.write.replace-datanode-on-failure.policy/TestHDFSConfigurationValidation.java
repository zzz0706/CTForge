package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure.Policy;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestHDFSConfigurationValidation {

    @Test
    // Test to validate HDFS configuration related to ReplaceDatanodeOnFailure policy
    // 1. Use the HDFS 2.8.5 API to correctly obtain configuration values instead of hardcoding.
    // 2. Prepare the test conditions.
    // 3. Validate configuration values and dependencies.
    // 4. Verify the correctness of expected outputs.
    public void testReplaceDatanodeOnFailurePolicyValidation() {
        // Prepare the configuration for testing
        Configuration conf = new HdfsConfiguration();

        // Step 1: Retrieve the configuration value for "dfs.client.block.write.replace-datanode-on-failure.policy"
        String policyValue = conf.get(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_DEFAULT
        );

        // Step 2: Validate that the retrieved value satisfies valid constraints
        // Valid values: DEFAULT, ALWAYS, NEVER, DISABLE
        boolean isValidPolicy = false;
        for (Policy policy : ReplaceDatanodeOnFailure.Policy.values()) {
            if (policy.name().equalsIgnoreCase(policyValue)) {
                isValidPolicy = true;
                break;
            }
        }
        assertTrue(
            "Invalid value for dfs.client.block.write.replace-datanode-on-failure.policy: " + policyValue,
            isValidPolicy
        );

        // Step 3: Check dependency on "dfs.client.block.write.replace-datanode-on-failure.enable"
        boolean isEnabled = conf.getBoolean(
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
            HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT
        );

        if (!isEnabled) {
            // If the feature is disabled, policy values should not be DEFAULT or ALWAYS
            assertFalse(
                "Policy should not be set to DEFAULT or ALWAYS when the feature is disabled.",
                policyValue.equalsIgnoreCase(Policy.DEFAULT.name()) || 
                policyValue.equalsIgnoreCase(Policy.ALWAYS.name())
            );
        }

        // Step 4: Validate constraints for minimum replication
        // Fix the issue by checking another existing key in HDFS 2.8.5.
        int minReplication = conf.getInt(
            HdfsClientConfigKeys.DFS_REPLICATION_KEY, // Correct key for minimum replication
            HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT // Default value for dfs.replication
        );
        assertTrue(
            "Invalid value for dfs.replication: " + minReplication,
            minReplication > 0 // Ensure minimum replication is positive
        );
    }
}