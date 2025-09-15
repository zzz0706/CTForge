package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

public class ZooKeeperServerMinSessionTimeoutTest {

    @Test
    //test code
    // 1. You need to use the ZooKeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions using mock components to isolate dependencies.
    // 3. Test code to verify minSessionTimeout propagation during initialization.
    // 4. Ensure proper cleanup after testing to close resources.
    public void test_ZooKeeperServer_minSessionTimeout_propagation() throws Exception {
        // Step 1: Create mock QuorumPeerConfig instance and configure mocked values.
        QuorumPeerConfig configMock = Mockito.mock(QuorumPeerConfig.class);
        File dataDir = new File("/tmp/zookeeper/data");
        File dataLogDir = new File("/tmp/zookeeper/logs");
        Mockito.when(configMock.getDataDir()).thenReturn(dataDir);
        Mockito.when(configMock.getDataLogDir()).thenReturn(dataLogDir);

        // Mock tickTime retrieval from config via API
        int tickTime = 2000;
        Mockito.when(configMock.getTickTime()).thenReturn(tickTime);

        // Mock minSessionTimeout retrieval using API
        Mockito.when(configMock.getMinSessionTimeout()).thenReturn(-1);

        // Step 2: Prepare mocked FileTxnSnapLog instance.
        FileTxnSnapLog txnLogMock = Mockito.mock(FileTxnSnapLog.class);
        Mockito.when(txnLogMock.getDataDir()).thenReturn(dataDir);
        Mockito.when(txnLogMock.getSnapDir()).thenReturn(dataLogDir);

        // Step 3: Initialize ZooKeeperServer with mocked values.
        ZooKeeperServer zkServer = new ZooKeeperServer(
            txnLogMock,
            tickTime,
            configMock.getMinSessionTimeout(),
            40000, // Default maxSessionTimeout value for this test
            null // ZKDatabase mock not necessary for this test
        );

        // Step 4: Verify that minSessionTimeout is properly initialized.
        int expectedMinSessionTimeout = tickTime * 2;
        assert zkServer.getMinSessionTimeout() == expectedMinSessionTimeout :
            "minSessionTimeout value not correctly initialized!";

        // Step 5: Clean up resources after testing.
        txnLogMock.close();
    }
}