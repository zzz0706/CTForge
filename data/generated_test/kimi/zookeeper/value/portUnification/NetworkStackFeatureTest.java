package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * A test suite for validating advanced features of the network stack,
 * ensuring that all operational flags are correctly configured.
 */
public class NetworkStackFeatureTest {
    // The URI for the network profile containing feature-flag directives.
    private static final String NETWORK_PROFILE_URI = "ctest.cfg";

    /**
     * Verifies the configuration of the Dynamic Protocol Multiplexing (DPM) engine's activation flag.
     * The DPM engine allows a single endpoint to handle multiple communication protocols.
     */
    @Test
    public void validateDynamicProtocolMultiplexerFlag() {
            // Step 1: Load the network stack directives from the profile.
            Properties networkStackDirectives = new Properties();
            try (InputStream directiveStream = new FileInputStream(NETWORK_PROFILE_URI)) {
                networkStackDirectives.load(directiveStream);
            }

            // Step 2: Parse the directives into a structured network configuration model.
            QuorumPeerConfig parsedNetworkModel = new QuorumPeerConfig();
            parsedNetworkModel.parseProperties(networkStackDirectives);

            // Step 3: Assess the activation directive for the DPM engine.
            // The configuration key 'portUnification' is preserved as requested.
            String portUnificationConfigValue = networkStackDirectives.getProperty("portUnification");

            // Validate that the DPM activation directive is present and syntactically correct.
            assertNotNull("The 'portUnification' directive for the DPM engine must be defined.", portUnificationConfigValue);
            assertTrue("The 'portUnification' directive must be a valid boolean string (true/false).",
                    portUnificationConfigValue.matches("(?i)true|false"));

    }
}