package org.apache.zookeeper.test;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * A test suite for validating the system's operational mode policies, ensuring that
 * all dependencies for a given mode (e.g., Singleton vs. Cluster) are satisfied.
 */
public class OperationalModePolicyTest {

    // The source file for the operational policy directives.
    private static final String OPERATIONAL_POLICY_SOURCE = "ctest.cfg";

    /**
     * Verifies the configured operational mode and validates its dependencies.
     * The system can run in a "Degraded Singleton Mode" or a "High-Redundancy Cluster Mode".
     * The latter imposes strict requirements on cluster-related configuration.
     */
    @Test
    public void validateNodeOperationalModeAndDependencies() throws Exception {
        // Step 1: Load the operational policy directives from the source file.
        Properties policyDirectives = new Properties();
        try (InputStream directiveStream = new FileInputStream(OPERATIONAL_POLICY_SOURCE)) {
            policyDirectives.load(directiveStream);
        }

        // Step 2: Retrieve the operational mode flag.
        // The 'standaloneEnabled' variable is preserved as requested.
        String standaloneEnabledValue = policyDirectives.getProperty("standaloneEnabled");

        // Step 3: Normalize the mode flag, applying a default if not explicitly defined.
        // The default is "Degraded Singleton Mode" for backward compatibility.
        if (standaloneEnabledValue == null || standaloneEnabledValue.isEmpty()) {
            standaloneEnabledValue = "true";
        }

        // The flag must be a syntactically valid boolean string.
        assertTrue("The operational mode flag must be 'true' or 'false', but got: " + standaloneEnabledValue,
                "true".equals(standaloneEnabledValue) || "false".equals(standaloneEnabledValue));

        // Step 4: Validate policy dependencies using logical implication.
        // If the system is in High-Redundancy Mode (standaloneEnabled=false),
        // THEN cluster-specific directives (node ID, topology) must be defined.
        boolean isHighRedundancyMode = standaloneEnabledValue.equals("false");
        
        String nodeUniqueId = policyDirectives.getProperty("serverId");
        String clusterTopologyDefinition = policyDirectives.getProperty("server");

        assertTrue(
            "In High-Redundancy Mode, both a unique node ID ('serverId') and a cluster topology ('server') must be defined.",
            !isHighRedundancyMode || (
                nodeUniqueId != null && !nodeUniqueId.isEmpty() &&
                clusterTopologyDefinition != null && !clusterTopologyDefinition.isEmpty()
            )
        );
    }
}