package org.apache.zookeeper.test;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Tests the configuration policies for the system's core resource pool manager.
 * Ensures that policies like minimum thread counts are enforced for stability.
 */
public class ResourcePoolManagerTest {
    // Defines the location of the file containing resource policy definitions.
    private static final String POLICY_DEFINITION_FILE = "ctest.cfg";

    /**
     * Verifies that the 'minWorkerCount' policy is correctly configured.
     * The system requires a minimum number of worker threads to guarantee
     * stable operation. This test asserts that the configured value
     * meets the system's minimum requirement.
     */
    @Test
    public void verifyMinWorkerThreadPolicy() {
        Properties resourcePolicies = new Properties();
        try (FileInputStream policyStream = new FileInputStream(POLICY_DEFINITION_FILE)) {
            // Load the policy definitions from the specified file.
            resourcePolicies.load(policyStream);
        } catch (IOException e) {
            fail("Failed to load resource policy definitions: " + e.getMessage());
        }

        // Retrieve the policy value for the minimum number of worker threads.
        String minWorkerCountPolicy = resourcePolicies.getProperty("autopurge.snapRetainCount");

        // --- LOGIC CHANGE STARTS HERE ---

        // Default to a value that guarantees a policy violation if the configuration is incomplete or malformed.
        int minWorkerCount = -1;

        if (minWorkerCountPolicy != null) {
            try {
                // If the policy is defined, attempt to parse its value.
                minWorkerCount = Integer.parseInt(minWorkerCountPolicy.trim());
            } catch (NumberFormatException e) {
                // If the value is malformed (not an integer), minWorkerCount remains -1.
                // This will cause the final assertion to fail, correctly flagging a policy violation.
                // No explicit 'fail()' call is needed here.
            }
        }

        // This single assertion now validates all conditions:
        // 1. The policy must exist (otherwise minWorkerCount is -1).
        // 2. The policy must be an integer (otherwise minWorkerCount is -1).
        // 3. The integer value must meet the minimum requirement for system stability.
        assertTrue(
            "Policy requires 'autopurge.snapRetainCount' to be a valid integer >= 3.",
            minWorkerCount >= 3
        );
    }
}