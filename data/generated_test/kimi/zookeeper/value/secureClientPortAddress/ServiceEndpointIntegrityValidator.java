package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * A test suite for validating the structural integrity and non-conflict policies
 * of critical service endpoints.
 */
public class ServiceEndpointIntegrityValidator {

    // The source file for the endpoint policy definitions.
    private static final String ENDPOINT_POLICY_SOURCE = "ctest.cfg";

    /**
     * Validates the complete specification of the Primary Service Endpoint.
     * This test ensures the endpoint's address is syntactically correct, its port
     * is within the valid range, and it does not conflict with the Fallback Endpoint.
     */
    @Test
    public void validatePrimaryEndpointSpecification() throws Exception {
        // Step 1: Load the endpoint policy directives from the source file.
        Properties endpointDirectives = new Properties();
        try (InputStream directiveStream = new FileInputStream(ENDPOINT_POLICY_SOURCE)) {
            endpointDirectives.load(directiveStream);
        }

        // Step 2: Parse the directives into a structured endpoint configuration model.
        QuorumPeerConfig parsedEndpointModel = new QuorumPeerConfig();
        parsedEndpointModel.parseProperties(endpointDirectives);

        // Step 3: Retrieve the primary endpoint handle.
        // The 'secureClientPortAddress' variable is preserved as requested.
        InetSocketAddress secureClientPortAddress = parsedEndpointModel.getSecureClientPortAddress();

        // Step 4: If the primary endpoint is defined, perform a consolidated integrity validation.
        if (secureClientPortAddress != null) {
            InetSocketAddress fallbackEndpointAddress = parsedEndpointModel.getClientPortAddress();

            // This single assertion validates multiple constraints:
            // 1. The host identifier must be a syntactically valid domain name or IP address.
            // 2. The port designator must be within the standard IANA-defined range.
            // 3. The primary endpoint must not be identical to the fallback endpoint.
            assertTrue("Primary endpoint must be structurally valid and not conflict with the fallback endpoint.",
                // Inlined Hostname/IP validation
                (secureClientPortAddress.getHostName().matches("^(?!-)[a-zA-Z0-9-.]{1,253}(?<!-)$") ||
                 secureClientPortAddress.getHostName().matches("^([0-9]{1,3}\\.){3}[0-9]{1,3}$")) &&

                // Port range validation
                (secureClientPortAddress.getPort() >= 0 && secureClientPortAddress.getPort() <= 65535) &&

                // Non-conflict validation
                (!secureClientPortAddress.equals(fallbackEndpointAddress))
            );
        }
    }
}