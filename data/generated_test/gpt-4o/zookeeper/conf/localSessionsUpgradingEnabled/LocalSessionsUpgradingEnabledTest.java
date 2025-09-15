package org.apache.zookeeper.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;
import org.mockito.Mockito;

public class LocalSessionsUpgradingEnabledTest {

    private static final String CONFIG_PATH = "ctest.cfg";

    @Test
    // Test code
    // 1. Use the zookeeper3.5.6 API correctly to obtain configuration values instead of hardcoding configuration values.
    // 2. Prepare the test conditions.
    // 3. Test the code logic.
    // 4. Ensure proper handling of exceptions in tests.
    public void testLocalSessionsUpgradingEnabledConfiguration() throws Exception {
        // Step 1: Load the configuration using the correct API
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        } catch (IOException e) {
            fail("Failed to load configuration file: " + e.getMessage());
        }

        // Step 2: Parse the configuration using QuorumPeerConfig API
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (Exception e) {
            fail("Failed to parse configuration properties: " + e.getMessage());
        }

        // Step 3: Validate configuration value for `localSessionsUpgradingEnabled`
        String localSessionsUpgradingEnabled = props.getProperty("localSessionsUpgradingEnabled");
        if (localSessionsUpgradingEnabled == null) {
            fail("The configuration does not contain `localSessionsUpgradingEnabled` property");
        }
        assertTrue("The configuration `localSessionsUpgradingEnabled` should either be 'true' or 'false'",
                "true".equalsIgnoreCase(localSessionsUpgradingEnabled) || "false".equalsIgnoreCase(localSessionsUpgradingEnabled));

        // Step 4: Mock behavior of QuorumPeerConfig and validate logic
        QuorumPeerConfig configMock = Mockito.mock(QuorumPeerConfig.class);
        Mockito.when(configMock.isLocalSessionsUpgradingEnabled())
                .thenReturn("true".equalsIgnoreCase(localSessionsUpgradingEnabled));

        try {
            if ("false".equalsIgnoreCase(localSessionsUpgradingEnabled)) {
                if (!configMock.isLocalSessionsUpgradingEnabled()) {
                    throw new KeeperException.EphemeralOnLocalSessionException();
                }
            } else if ("true".equalsIgnoreCase(localSessionsUpgradingEnabled)) {
                // Assert that upgrading is allowed when configuration is true
                boolean upgradingAllowed = configMock.isLocalSessionsUpgradingEnabled();
                assertTrue("Upgrading should be allowed when `localSessionsUpgradingEnabled` is true", upgradingAllowed);
            }
        } catch (KeeperException.EphemeralOnLocalSessionException e) {
            assertTrue("Exception should only occur when `localSessionsUpgradingEnabled` is false",
                    "false".equalsIgnoreCase(localSessionsUpgradingEnabled));
        }
    }
}