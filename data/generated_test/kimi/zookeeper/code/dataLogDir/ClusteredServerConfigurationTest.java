package org.apache.zookeeper.test;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Verifies the configuration parsing and mock interactions for a ZooKeeper server
 * intended to run in a clustered (quorum) mode.
 */
public class ClusteredServerConfigurationTest {

    /**
     * This test ensures that when provided with properties for a clustered setup,
     * the QuorumPeerConfig correctly parses them and that related mock components
     * behave as expected.
     */
    @Test
    public void testConfigurationParsingAndMockInteractionsForQuorumMode() throws IOException, QuorumPeerConfig.ConfigException {
        // Step 1: Prepare the test configuration and mock environment.
        Properties configurationProperties = createQuorumModeProperties();
        QuorumPeerConfig clusterMemberConfig = new QuorumPeerConfig();

        // Step 2: Parse the properties into the configuration object.
        clusterMemberConfig.parseProperties(configurationProperties);

        // Step 3: Set up mock services that would interact with this configuration.
        DatadirCleanupManager mockMaintenanceService = mock(DatadirCleanupManager.class);
        when(mockMaintenanceService.isStarted()).thenReturn(true);

        ZooKeeperServer mockZooKeeperServer = mock(ZooKeeperServer.class);
        FileTxnSnapLog mockTxnLog = mock(FileTxnSnapLog.class);
        when(mockZooKeeperServer.getTxnLogFactory()).thenReturn(mockTxnLog);

        // Step 4: Validate the parsed configuration values and mock behaviors.
        assertEquals("Snapshot directory was not parsed correctly.",
                configurationProperties.getProperty("dataDir"),
                clusterMemberConfig.getDataDir().getAbsolutePath());

        assertEquals("Transaction log directory was not parsed correctly.",
                configurationProperties.getProperty("dataLogDir"),
                clusterMemberConfig.getDataLogDir().getAbsolutePath());

        assertTrue("Mocked maintenance service should report as started.",
                mockMaintenanceService.isStarted());
        
        // Verify that the isStarted method was indeed called during the assertion.
        verify(mockMaintenanceService, times(1)).isStarted();
    }

    /**
     * A helper factory method to create a standard set of properties for a
     * ZooKeeper server running in quorum mode.
     *
     * @return A {@link Properties} object populated with test data.
     */
    private Properties createQuorumModeProperties() {
        Properties props = new Properties();
        File snapshotDir = new File("build/test/snapshots");
        File txnLogDir = new File("build/test/logs");

        // Ensure parent directories exist to avoid potential IOExceptions on getAbsolutePath()
        snapshotDir.mkdirs();
        txnLogDir.mkdirs();

        props.setProperty("dataDir", snapshotDir.getAbsolutePath());
        props.setProperty("dataLogDir", txnLogDir.getAbsolutePath());
        props.setProperty("snapRetainCount", "3");
        props.setProperty("purgeInterval", "1");
        // Add minimal server list to imply a quorum configuration
        props.setProperty("server.1", "localhost:2888:3888");

        return props;
    }
}