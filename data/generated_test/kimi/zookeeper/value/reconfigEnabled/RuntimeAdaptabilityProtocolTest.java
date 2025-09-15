package org.apache.zookeeper.test;

import org.junit.Test;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * A test suite for validating the system's runtime adaptability protocols.
 * These tests ensure that dynamic behavior flags are correctly interpreted.
 */
public class RuntimeAdaptabilityProtocolTest {

    // The source file for the runtime adaptability policy directives.
    private static final String ADAPTABILITY_POLICY_SOURCE = "ctest.cfg";

    /**
     * Verifies the correctness of the 'reconfigEnabled' flag, which governs
     * the system's core adaptability protocol.
     */
    @Test
    public void validateAdaptabilityProtocolFlag() {
        // Load the policy directives from the source file.
        Properties policyDirectives = new Properties();
        try (InputStream policyStream = new FileInputStream(ADAPTABILITY_POLICY_SOURCE)) {
            policyDirectives.load(policyStream);
        } catch (IOException e) {
            // If the policy source is inaccessible, it constitutes a critical failure.
            throw new RuntimeException("Error accessing adaptability policy source: " + ADAPTABILITY_POLICY_SOURCE, e);
        }

        // Step 1: Extract the 'reconfigEnabled' protocol flag from the policy directives.
        // The variable name is preserved as requested.
        String reconfigEnabledValue = policyDirectives.getProperty("reconfigEnabled");

        // --- Start of Inlined Validation Logic ---

        // Policy Constraint:
        // - Acceptable values are strictly "true" or "false" (case-sensitive).
        // - If the flag is not explicitly defined, the protocol defaults to a disabled state ("false").
        if (reconfigEnabledValue == null || reconfigEnabledValue.isEmpty()) {
            // Apply the default state if the protocol flag is not explicitly configured.
            reconfigEnabledValue = "false";
        }

        // Assert that the final value conforms to the strict boolean string format.
        assertTrue(
            "Invalid value for 'reconfigEnabled' protocol flag, expected 'true' or 'false', but got: " + reconfigEnabledValue,
            "true".equals(reconfigEnabledValue) || "false".equals(reconfigEnabledValue)
        );
        
    }
}