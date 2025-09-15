package org.apache.zookeeper.server.quorum.auth;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A test suite for validating the system's tiered access control policies,
 * ensuring that authentication mandates for different node tiers are enforced.
 */
public class TieredAccessControlPolicyTest {

    // The source file for the access control policy definition.
    private static final String ACCESS_CONTROL_DEFINITION_FILE = "ctest.cfg";

    /**
     * Verifies the authentication enforcement flag for subordinate (secondary tier) system nodes.
     * This policy dictates whether subordinate nodes must use strong authentication protocols.
     */
    @Test
    public void validateSubordinateNodeAuthEnforcementFlag() {
        try {
            // Step 1: Load the access control directives from the policy file.
            Properties accessControlDirectives = new Properties();
            try (InputStream directiveStream = new FileInputStream(ACCESS_CONTROL_DEFINITION_FILE)) {
                accessControlDirectives.load(directiveStream);
            }

            // Step 2: Parse the directives into a structured security policy model.
            QuorumPeerConfig parsedSecurityPolicyModel = new QuorumPeerConfig();
            parsedSecurityPolicyModel.parseProperties(accessControlDirectives);

            // Step 3: Fetch the value of the subordinate node authentication enforcement flag.
            // The configuration key is preserved as requested.
            String quorumLearnerRequireSaslValue = accessControlDirectives.getProperty(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED);

            // Step 4: Validate the flag's syntax. It must be a non-null, valid boolean string.
            assertTrue(
                String.format("Invalid value for policy 'quorum.auth.learnerRequireSasl'. Expected a valid boolean string, but found: '%s'",
                    QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, quorumLearnerRequireSaslValue),
                quorumLearnerRequireSaslValue != null && quorumLearnerRequireSaslValue.matches("(?i)true|false")
            );

        } catch (Exception e) {
            fail("Validation of the access control policy failed due to an exception: " + e.getMessage());
        }
    }
}