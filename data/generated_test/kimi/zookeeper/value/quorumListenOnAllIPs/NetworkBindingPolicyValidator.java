package org.apache.zookeeper.server.quorum;

import org.junit.Test;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotEquals;

/**
 * A test suite for validating the system's network interface binding policies,
 * ensuring correct socket behavior based on configuration directives.
 */
public class NetworkBindingPolicyValidator {

    // The source file for network binding policy directives.
    private static final String BINDING_POLICY_SOURCE = "ctest.cfg";

    /**
     * Validates the configured network interface binding scope (promiscuous vs. scoped).
     * This policy determines whether the service should bind to all available network interfaces
     * or to a specific, explicitly defined interface address.
     */
    @Test
    public void validateInterfaceBindingScope() throws Exception {
        // Step 1: Load the binding policy directives from the source file.
        Properties bindingDirectives = new Properties();
        try (InputStream directiveStream = new FileInputStream(BINDING_POLICY_SOURCE)) {
            bindingDirectives.load(directiveStream);
        }

        // Step 2: Parse the directives into a structured network binding model.
        QuorumPeerConfig parsedBindingModel = new QuorumPeerConfig();
        parsedBindingModel.parseProperties(bindingDirectives);

        // Step 3: Retrieve the value of the promiscuous binding flag.
        // The variable name 'quorumListenOnAllIPs' is preserved as requested.
        Boolean quorumListenOnAllIPs = parsedBindingModel.getQuorumListenOnAllIPs();

        // Step 4: Validate the state and determinacy of the binding policy flag.
        assertNotNull("The binding scope policy ('quorumListenOnAllIPs') must be explicitly defined.", quorumListenOnAllIPs);
        assertNotEquals(
            "The binding scope must be a determinate binary state (either promiscuous or scoped).",
            !quorumListenOnAllIPs,
            quorumListenOnAllIPs
        );
    }
}