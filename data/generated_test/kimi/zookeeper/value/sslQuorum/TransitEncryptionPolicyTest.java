package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A test suite for validating the system's data-in-transit encryption policies,
 * ensuring secure communication channels are configured as expected.
 */
public class TransitEncryptionPolicyTest {

    // The source file for the encryption policy directives.
    private static final String ENCRYPTION_POLICY_SOURCE = "ctest.cfg";

    /**
     * Verifies the activation flag for the data-in-transit encryption layer.
     * This policy determines whether inter-node communication must be encrypted.
     */
    @Test
    public void validateDataInTransitEncryptionFlag() throws Exception {
        // Step 1: Load the encryption policy directives from the source file.
        Properties policyDirectives = new Properties();
        try (InputStream directiveStream = new FileInputStream(ENCRYPTION_POLICY_SOURCE)) {
            policyDirectives.load(directiveStream);
        }

        // Step 2: Parse the directives into a structured security configuration model.
        QuorumPeerConfig parsedSecurityModel = new QuorumPeerConfig();
        parsedSecurityModel.parseProperties(policyDirectives);

        // Step 3: Determine the expected state of the encryption layer based on the policy.
        // The 'sslQuorum' variable and key are preserved as requested.
        String sslQuorumValue = policyDirectives.getProperty("sslQuorum");
        
        // The encryption layer is disabled by default if the policy flag is not explicitly set.
        boolean expectedEncryptionState = false;

        if (sslQuorumValue != null && !sslQuorumValue.trim().isEmpty()) {
            String sanitizedFlagValue = sslQuorumValue.trim().toLowerCase();
            
            // The flag must be a syntactically valid boolean string.
            assertTrue("The encryption flag must be a valid boolean string ('true' or 'false').",
                       sanitizedFlagValue.equals("true") || sanitizedFlagValue.equals("false"));
            
            // Derive the expected state from the sanitized flag value.
            expectedEncryptionState = Boolean.parseBoolean(sanitizedFlagValue);
        }

        // Step 4: Assert that the runtime encryption state matches the state derived from the policy.
        assertEquals(
            "The system's reported encryption state must accurately reflect the configured policy.",
            expectedEncryptionState,
            parsedSecurityModel.isSslQuorum()
        );
    }
}