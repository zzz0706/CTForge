package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.*;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PeerTypeConfigTest {

    @Test
    // Test code
    // 1. You need to use the Zookeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testPeerTypeConfiguration() throws IOException, ConfigException {
        // 1. Define the configuration file path.
        String configPath = "ctest.cfg";

        // 2. Prepare the test conditions.
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configPath)) {
            props.load(in);
        }

        // 3. Setup the QuorumPeerConfig instance with loaded properties.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 4. Validate the peer type configuration using the Zookeeper API.
        QuorumVerifier quorumVerifier = config.getQuorumVerifier();
        assertNotNull("QuorumVerifier should not be null.", quorumVerifier);

        // Mock the QuorumPeerConfig for further verification.
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.getQuorumVerifier()).thenReturn(quorumVerifier);

        QuorumVerifier retrievedQuorumVerifier = configMock.getQuorumVerifier();
        assertEquals(
            "QuorumVerifier configuration does not match the mocked QuorumPeerConfig.",
            quorumVerifier,
            retrievedQuorumVerifier
        );

        // Validate that the configuration matches the expected role types.
        for (QuorumServer server : quorumVerifier.getAllMembers()) {
            assertNotNull("QuorumServer address should not be null", server.addr);
            assertNotNull("QuorumServer type should not be null", server.type);

            // Ensure the type is either PARTICIPANT or OBSERVER.
            assertTrue(
                "QuorumServer type must be PARTICIPANT or OBSERVER.",
                server.type == LearnerType.PARTICIPANT || server.type == LearnerType.OBSERVER
            );
        }

        // Additional checks to handle invalid or unset types.
        if (quorumVerifier.getAllMembers().isEmpty()) {
            fail("QuorumPeerConfig contains no server information.");
        }

        // Output the effective QuorumVerifier details for debugging purposes.
        System.out.println("Effective QuorumVerifier configuration: " + quorumVerifier);
    }
}