package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class QuorumAuthConfigTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    /**
     * Test to validate the `quorum.auth.serverRequireSasl` configuration.
     */
    @Test
    public void testQuorumAuthServerRequireSaslConfig() throws Exception {

        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(props);

        String quorumAuthServerRequireSasl = props.getProperty(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);

        quorumAuthServerRequireSasl = quorumAuthServerRequireSasl.trim().toLowerCase();
        assertTrue(
                "The configuration 'quorum.auth.serverRequireSasl' must be 'true' or 'false'.",
                "true".equals(quorumAuthServerRequireSasl) || "false".equals(quorumAuthServerRequireSasl));

    }
}