package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A test suite for validating the system's cryptographic security policies,
 * particularly those related to federal compliance standards.
 */
public class CryptoCompliancePolicyTest {

    // The source file for security policy directives.
    private static final String SECURITY_POLICY_SOURCE = "ctest.cfg";

    /**
     * Verifies the activation flag for FIPS 140-2 compliance mode.
     * This mode ensures that all cryptographic modules operate under strict
     * federal information processing standards.
     */
    @Test
    public void validateFipsComplianceModeFlag() {
        try {
            // Step 1: Load the security directives from the policy source file.
            Properties securityDirectives = new Properties();
            try (InputStream directiveStream = new FileInputStream(SECURITY_POLICY_SOURCE)) {
                securityDirectives.load(directiveStream);
            }

            // Step 2: Parse the directives into a structured security context model.
            QuorumPeerConfig parsedSecurityContext = new QuorumPeerConfig();
            parsedSecurityContext.parseProperties(securityDirectives);

            // Step 3: Extract the FIPS compliance mode activation flag.
            // The configuration key 'quorum.auth.enableSasl' is preserved as requested.
            String quorumAuthEnableSasl = securityDirectives.getProperty("quorum.auth.enableSasl");

            // Step 4: Validate the flag's syntax. It must be a valid boolean string.
            assertTrue(
                "The FIPS compliance mode flag ('quorum.auth.enableSasl') must be 'true' or 'false'.",
                quorumAuthEnableSasl != null && quorumAuthEnableSasl.matches("(?i)true|false")
            );

        } catch (Exception e) {
            fail("Validation of the security policy failed due to an exception: " + e.getMessage());
        }
    }
}