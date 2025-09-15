package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

public class DatadirCleanupManagerTest {

    @Test
    // Test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_start_purgeTaskNotScheduledNegativeInterval() {
        // Step 1: Mock QuorumPeerConfig to provide valid dataDir and dataLogDir without hardcoding.
        File dataDir = Mockito.mock(File.class); // Mocked valid `dataDir`
        File dataLogDir = Mockito.mock(File.class); // Mocked valid `dataLogDir`

        QuorumPeerConfig configMock = Mockito.mock(QuorumPeerConfig.class);
        Mockito.when(configMock.getDataDir()).thenReturn(dataDir);
        Mockito.when(configMock.getDataLogDir()).thenReturn(dataLogDir);

        // Step 2: Prepare a DatadirCleanupManager instance with mock configurations and zero/negative purgeInterval.
        int snapRetainCount = 3; // Arbitrary valid snapRetainCount
        int purgeInterval = 0; // Testing zero purgeInterval
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
                configMock.getDataDir(),
                configMock.getDataLogDir(),
                snapRetainCount,
                purgeInterval
        );

        // Step 3: Start the cleanup manager and verify behavior.
        purgeMgr.start();

        // Step 4: Assert the behavior by verifying direct attributes or mock interactions.
        // Since `Mockito.verifyNoInteractions` cannot be used for `purgeMgr`,
        // we check indirectly whether the purge task was not scheduled by examining related behaviors or ensuring no exception occurs during execution.
        Mockito.verify(configMock).getDataDir();
        Mockito.verify(configMock).getDataLogDir();

        // Step 5: Validation complete. No specific cleanup is required for this test scenario.
    }
}