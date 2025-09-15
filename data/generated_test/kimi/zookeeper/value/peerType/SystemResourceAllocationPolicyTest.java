package org.apache.zookeeper.test;

import org.junit.Test;
import org.junit.Assert;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;


public class SystemResourceAllocationPolicyTest {

    // Defines the source file for the resource policy definitions.
    private static final String RESOURCE_POLICY_DEFINITION_FILE = "ctest.cfg";

    /**
     * Validates that the maximum permissible resource quota adheres to the
     * constraints defined by the base allocation unit.
     */
    @Test
    public void validateMaximumResourceQuotaConstraint() {
        try {
            // Step 1: Load the policy directives from the definition file.
            Properties policyDirectives = new Properties();
            try (InputStream directiveStream = new FileInputStream(RESOURCE_POLICY_DEFINITION_FILE)) {
                policyDirectives.load(directiveStream);
            }

            // Step 2: Parse the directives into a structured system policy model.
            QuorumPeerConfig systemPolicyModel = new QuorumPeerConfig();
            systemPolicyModel.parseProperties(policyDirectives);

            // Step 3: Fetch the fundamental resource allocation metrics.
            int baseAllocationUnitMillis = systemPolicyModel.getTickTime(); // The smallest schedulable time unit.
            int maxSessionTimeout = systemPolicyModel.getMaxSessionTimeout(); // The max quota. This name is preserved.

            // Step 4: Perform a consolidated validation of the quota constraints.
            // The maximum quota must be a positive multiple of the base unit, with a minimum
            // multiplier of 20 to ensure sufficient resource availability.
            Assert.assertTrue(
                "The maximum resource quota must be a positive value and at least 20 times the base allocation unit.",
                baseAllocationUnitMillis > 0 && maxSessionTimeout >= (baseAllocationUnitMillis * 20)
            );

        } catch (Exception e) {
            Assert.fail("Validation of resource allocation policy failed due to an exception: " + e.getMessage());
        }
    }
}