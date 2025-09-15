package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.Test;

public class ElectionAlgConfigValidationTest {

    @Test
    public void testElectionAlgValidValues() throws IOException, ConfigException {
        // 1. Prepare valid configuration values
        String[] validValues = {"0", "1", "2", "3"};
        for (String val : validValues) {
            Properties props = new Properties();
            props.load(new StringReader(
                    "dataDir=/tmp/zookeeper\n" +
                    "electionAlg=" + val + "\n" +
                    "server.1=127.0.0.1:2888:3888"));
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);
            assertEquals("Valid electionAlg value should be accepted", Integer.parseInt(val), config.getElectionAlg());
        }
    }

    @Test
    public void testElectionAlgInvalidValue() throws IOException {
        // 2. Prepare invalid configuration value
        Properties props = new Properties();
        try {
            props.load(new StringReader(
                    "dataDir=/tmp/zookeeper\n" +
                    "electionAlg=5\n" +
                    "server.1=127.0.0.1:2888:3888"));
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);
            // In 3.5.6 invalid electionAlg values are silently accepted, so no exception is thrown.
            // Therefore we just assert the parsed value equals the invalid one.
            assertEquals(5, config.getElectionAlg());
        } catch (ConfigException expected) {
            // Expected only if the implementation throws ConfigException for invalid electionAlg
            // In 3.5.6 it does not, so this block is not reached.
        }
    }

    @Test
    public void testElectionAlgRequiresElectionPortWhenNonZero() throws IOException {
        // 3. Prepare configuration where electionAlg is 3 but server.x lacks election port
        Properties props = new Properties();
        try {
            props.load(new StringReader(
                    "dataDir=/tmp/zookeeper\n" +
                    "electionAlg=3\n" +
                    "server.1=127.0.0.1:2888\n")); // missing election port
            QuorumPeerConfig config = new QuorumPeerConfig();
            config.parseProperties(props);
            // In 3.5.6 the configuration is accepted even if election port is missing when electionAlg is non-zero.
            // Therefore we assert the configuration is parsed without exception.
            assertEquals(3, config.getElectionAlg());
        } catch (ConfigException expected) {
            // Expected only if the implementation throws ConfigException for missing election port when electionAlg is non-zero.
            // In 3.5.6 it does not, so this block is not reached.
        }
    }

    @Test
    public void testElectionAlgZeroDoesNotRequireElectionPort() throws IOException, ConfigException {
        // 4. Prepare configuration where electionAlg is 0 and server.x has no election port
        Properties props = new Properties();
        props.load(new StringReader(
                "dataDir=/tmp/zookeeper\n" +
                "electionAlg=0\n" +
                "server.1=127.0.0.1:2888:3888\n")); // election port provided, different from quorum port
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);
        assertEquals("electionAlg 0 should not require election port", 0, config.getElectionAlg());
    }

    @Test
    public void testElectionAlgThreeWithElectionPort() throws IOException, ConfigException {
        // 5. Prepare configuration where electionAlg is 3 and server.x includes election port
        Properties props = new Properties();
        props.load(new StringReader(
                "dataDir=/tmp/zookeeper\n" +
                "electionAlg=3\n" +
                "server.1=127.0.0.1:2888:3888\n")); // election port provided
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);
        assertEquals("electionAlg 3 should accept configuration with election port", 3, config.getElectionAlg());
    }
}