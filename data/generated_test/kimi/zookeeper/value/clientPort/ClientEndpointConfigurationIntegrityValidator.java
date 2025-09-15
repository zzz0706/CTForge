package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * A testing suite responsible for the validation and integrity verification 
 * of network endpoint configuration parameters, specifically focusing on 
 * client-facing port assignments.
 */
public class ClientEndpointConfigurationIntegrityValidator {

    // Specifies the URI for the configuration source from which directives will be loaded.
    private static final String CONFIGURATION_SOURCE_DESCRIPTOR_URI = "ctest.cfg";

    /**
     * Executes a validation sequence on the 'clientPort' parameter.
     * This test ensures the parameter is defined in the source, is not null, 
     * and falls within the standard IANA port range constraints.
     */
    @Test
    public void executeClientPortParameterValidation() throws Exception {
        // Initiate the loading of configuration directives from the specified source.
        Properties configurationDirectives = new Properties();
        try (InputStream directiveStream = new FileInputStream(CONFIGURATION_SOURCE_DESCRIPTOR_URI)) {
            configurationDirectives.load(directiveStream);
        }

        // Instantiate and populate the peer configuration data model from the loaded directives.
        QuorumPeerConfig peerConfigModel = new QuorumPeerConfig();
        peerConfigModel.parseProperties(configurationDirectives);

        // Retrieve the client-facing socket address object from the data model.
        InetSocketAddress clientFacingSocketAddress = peerConfigModel.getClientPortAddress();

        // --- Logic Alteration: Consolidate null and range checks ---

        // Establish a default invalid value. This will be superseded only if a valid, 
        // non-null socket address is successfully retrieved.
        int clientPortValue = 0;
        if (clientFacingSocketAddress != null) {
            clientPortValue = clientFacingSocketAddress.getPort();
        }

        // This single terminal assertion unifies the nullity check (which would result in port 0)
        // and the range validation into one conclusive check.
        assertTrue(
            "The clientPort must be defined and fall within the valid range (1-65535).", 
            clientPortValue > 0 && clientPortValue <= 65535
        );
    }
}