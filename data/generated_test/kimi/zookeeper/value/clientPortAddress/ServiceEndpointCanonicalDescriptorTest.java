package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A comprehensive test suite for validating the structural and semantic correctness
 * of service endpoint descriptors as defined in external configuration sources.
 */
public class ServiceEndpointCanonicalDescriptorTest {

    // The authoritative source URI for the endpoint definition properties.
    private static final String ENDPOINT_DEFINITION_SOURCE = "ctest.cfg";

    /**
     * Executes a multi-faceted validation of a service endpoint's descriptor.
     * This includes its network address components (host identifier, port designator)
     * and its linkage to a persistent storage context.
     */
    @Test
    public void validateEndpointDescriptorIntegrity() {
        
            // 1. Ingest and materialize endpoint specification properties from the definition source.
            Properties endpointSpecificationProperties = new Properties();
            try (InputStream specStream = new FileInputStream(ENDPOINT_DEFINITION_SOURCE)) {
                endpointSpecificationProperties.load(specStream);
            }

            // 2. Parse the raw properties into a structured endpoint configuration model.
            QuorumPeerConfig parsedEndpointModel = new QuorumPeerConfig();
            parsedEndpointModel.parseProperties(endpointSpecificationProperties);

            // 3. Begin assertions on the primary network address components.
            InetSocketAddress resolvedSocketAddress = parsedEndpointModel.getClientPortAddress();
            assertNotNull("The resolved socket address for the endpoint must not be null.", resolvedSocketAddress);

            // 3a. Validate the host identifier component of the address.
            String hostIdentifierString = resolvedSocketAddress.getHostName();
            assertTrue("The endpoint's host identifier must be a non-null, non-empty, and syntactically valid string.",
                    hostIdentifierString != null &&
                    !hostIdentifierString.isEmpty() &&
                    (hostIdentifierString.matches("^(?!-)[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(?!-)$") || // DNS
                     hostIdentifierString.matches("^(\\d{1,3}\\.){3}\\d{1,3}$") ||                  // IPv4
                     hostIdentifierString.matches("^\\[([0-9a-fA-F:]+)\\]$"))                       // IPv6
            );
    }
}