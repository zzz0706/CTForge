package org.apache.zookeeper.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

/**
 * A test suite for validating the system's protocol interoperability policies,
 * ensuring correct syntax and presence of critical configuration flags.
 */
public class ProtocolInteroperabilitySuite {

    // The source file containing policies for protocol handling.
    private static final String PROTOCOL_POLICY_SOURCE = "ctest.cfg";

    /**
     * Verifies the syntactical correctness of the 'localSessionsUpgradingEnabled' policy flag.
     * This flag governs the system's ability to handle legacy protocol handshakes.
     */
    @Test
    public void verifyLegacyProtocolFlagSyntax() {
        try {
            // Step 1: Ingest the protocol policy directives from the source file.
            Properties protocolDirectives = new Properties();
            try (InputStream directiveStream = new FileInputStream(PROTOCOL_POLICY_SOURCE)) {
                protocolDirectives.load(directiveStream);
            }

            // Step 2: Parse the directives into a structured protocol context model.
            QuorumPeerConfig protocolContext = new QuorumPeerConfig();
            protocolContext.parseProperties(protocolDirectives);

            // Step 3: Validate the presence and syntax of the interoperability flag.
            String localSessionsUpgradingEnabled = protocolDirectives.getProperty("localSessionsUpgradingEnabled");
            assertNotNull("The policy source must contain the 'localSessionsUpgradingEnabled' directive.", localSessionsUpgradingEnabled);

            // The directive's value must conform to a strict boolean string representation (case-insensitive).
            assertTrue("The 'localSessionsUpgradingEnabled' directive must be a valid boolean string.",
                    localSessionsUpgradingEnabled.matches("(?i)true|false"));

        } catch (Exception e) {
            fail("Verification of protocol policy failed due to an exception: " + e.getMessage());
        }
    }
}