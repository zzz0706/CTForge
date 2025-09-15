package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class QuorumPeerConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Utility method to load ZooKeeper configuration from a file.
     *
     * @return Properties object representing ZooKeeper configuration.
     * @throws IOException if there is an error reading the configuration file.
     */
    private Properties loadConfiguration() throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }
        return props;
    }

    /**
     * Tests if the "electionAlg" configuration parameter is valid.
     * Ensures the algorithm falls within the allowed set.
     */
    @Test
    public void testElectionAlgValidity() throws Exception {
        // 1. Load configuration from file.
        Properties props = loadConfiguration();

        // 2. Parse the configuration using QuorumPeerConfig.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 3. Test the election algorithm configuration.
        int electionAlg = config.getElectionAlg();
        assertTrue(
            "electionAlg must be one of {0, 1, 2, 3}",
            electionAlg == 0 || electionAlg == 1 || electionAlg == 2 || electionAlg == 3
        );

        // 4. Additional check for deprecated algorithms.
        if (electionAlg == 0 || electionAlg == 1 || electionAlg == 2) {
            System.out.println("Warning: electionAlg value " + electionAlg + " is deprecated.");
        }
    }

    /**
     * Tests if the dependency between "electionAlg" and "server.x" configuration
     * is valid. Ensures that "server.x" configuration includes valid election
     * ports if the election algorithm is not 0.
     */
    @Test
    public void testElectionAlgDependency() throws Exception {
        // 1. Load configuration from file.
        Properties props = loadConfiguration();

        // 2. Parse the configuration using QuorumPeerConfig.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 3. Test dependency between "electionAlg" and "server.x".
        int electionAlg = config.getElectionAlg();
        if (config.getQuorumVerifier() instanceof QuorumMaj) {
            QuorumMaj quorumVerifier = (QuorumMaj) config.getQuorumVerifier();
            if (electionAlg != 0) {
                for (Map.Entry<Long, QuorumServer> entry : quorumVerifier.getVotingMembers().entrySet()) {
                    QuorumServer server = entry.getValue();
                    InetSocketAddress electionAddr = server.electionAddr;
                    assertNotNull(
                        "Election port must be specified for server: " + entry.getKey(),
                        electionAddr
                    );
                }
            }
        }
    }

    /**
     * Tests general configuration and validity of other attributes.
     */
    @Test
    public void testGeneralConfigurationValidity() throws Exception {
        // 1. Load configuration from file.
        Properties props = loadConfiguration();

        // 2. Parse the configuration using QuorumPeerConfig.
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        // 3. Validate client port.
        InetSocketAddress clientPortAddr = config.getClientPortAddress();
        assertNotNull("Client port address must be present.", clientPortAddr);
        assertTrue(
            "Client port must be in valid range (1-65535).",
            clientPortAddr.getPort() >= 1 && clientPortAddr.getPort() <= 65535
        );

        // 4. Validate standaloneEnabled behavior.
        if (config.getQuorumVerifier() instanceof QuorumMaj) {
            QuorumMaj quorumVerifier = (QuorumMaj) config.getQuorumVerifier();
            int numParticipators = quorumVerifier.getVotingMembers().size();
            int numObservers = quorumVerifier.getObservingMembers().size();
            boolean standaloneEnabled = config.isStandaloneEnabled();

            if (numParticipators == 0) {
                if (!standaloneEnabled) {
                    fail("standaloneEnabled = false and number of participants is zero; invalid configuration.");
                }
                assertEquals("Zero participants implies no observers.", 0, numObservers);
            } else if (numParticipators == 1 && standaloneEnabled) {
                System.out.println("Warning: Only one server specified, standalone will be enabled.");
                assertEquals("Single participant implies no observers.", 0, numObservers);
            } else {
                if (numParticipators <= 2) {
                    System.out.println("Warning: Inadequate fault tolerance. Minimum quorum size is 3.");
                }
                if (numParticipators % 2 == 0) {
                    System.out.println("Warning: Non-optimal quorum. Consider an odd number of servers.");
                }
            }
        }
    }
}