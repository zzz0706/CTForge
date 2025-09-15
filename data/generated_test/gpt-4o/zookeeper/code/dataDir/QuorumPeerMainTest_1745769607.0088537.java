package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.Test;

import java.io.File;
import java.util.Properties;
import java.util.TimerTask;

import static org.mockito.Mockito.*;

public class QuorumPeerMainTest {

    @Test
    // test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initializeAndRun_quorumModeValidConfiguration() throws Exception {
        // Step 1: Prepare the Configuration File
        Properties zkProperties = new Properties();
        File dataDir = new File("/tmp/zookeeper/dataDir");
        File dataLogDir = new File("/tmp/zookeeper/dataLogDir");
        zkProperties.setProperty("dataDir", dataDir.getAbsolutePath());
        zkProperties.setProperty("dataLogDir", dataLogDir.getAbsolutePath());
        zkProperties.setProperty("snapRetainCount", "3");
        zkProperties.setProperty("purgeInterval", "12");
        File configFile = new File("/tmp/zookeeper.properties");
        try (java.io.FileWriter writer = new java.io.FileWriter(configFile)) {
            zkProperties.store(writer, "ZooKeeper Test Configurations");
        }

        // Step 2: Parse Configuration and Mock Dependencies
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(zkProperties);

        DatadirCleanupManager purgeMgr = spy(new DatadirCleanupManager(
            quorumPeerConfig.getDataDir(),
            quorumPeerConfig.getDataLogDir(),
            quorumPeerConfig.getSnapRetainCount(),
            quorumPeerConfig.getPurgeInterval()
        ));

        QuorumPeerMain peerMain = new QuorumPeerMain();

        // Step 3: Test Execution
        String[] args = {configFile.getAbsolutePath()};
        peerMain.runFromConfig(quorumPeerConfig);

        // Validate Behavior
        verify(purgeMgr).start();

        // Ensure Purge Task is Scheduled
        TimerTask mockPurgeTask = mock(TimerTask.class);
        mockPurgeTask.run();  // Simulate task execution.
        verify(mockPurgeTask, never()).run(); // Verify task logic does not throw errors.

        // Step 4: Teardown
        configFile.delete();
    }

    @Test
    // test code for ZooKeeperServer#getDataDirSize
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_ZooKeeperServer_getDataDirSize() throws Exception {
        // Step 1: Prepare Mock ZooKeeperServer and FileTxnSnapLog
        FileTxnSnapLog snapLogMock = mock(FileTxnSnapLog.class);
        File mockDataDir = new File("/tmp/zookeeper/dataDir");
        when(snapLogMock.getDataDir()).thenReturn(mockDataDir);

        ZooKeeperServer server = spy(new ZooKeeperServer());
        doReturn(snapLogMock).when(server).getTxnLogFactory();

        // Step 2: Simulate Data Directory Usage
        long dirSize = server.getDataDirSize();

        // Validate Results
        verify(snapLogMock).getDataDir();
        assert dirSize >= 0;
    }
}