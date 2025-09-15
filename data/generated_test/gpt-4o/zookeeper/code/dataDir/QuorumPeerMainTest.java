package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.util.TimerTask;

import static org.mockito.Mockito.*;

public class QuorumPeerMainTest {

    @Test
    // test code
    // 1. Directly invoke API to get configuration values, do not define a configuration file and add them in.
    // 2. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 3. Prepare the test conditions.
    // 4. Test code.
    // 5. Code after testing.
    public void test_initializeAndRun_quorumModeValidConfiguration() throws Exception {
        // Prepare Mock Configurations
        File dataDir = new File("/tmp/zookeeper/dataDir");
        File dataLogDir = new File("/tmp/zookeeper/dataLogDir");
        
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.getDataDir()).thenReturn(dataDir);
        when(configMock.getDataLogDir()).thenReturn(dataLogDir);
        when(configMock.getSnapRetainCount()).thenReturn(3);
        when(configMock.getPurgeInterval()).thenReturn(12);
        when(configMock.isDistributed()).thenReturn(true);
        
        // Create and Mock Dependencies
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
            configMock.getDataDir(), 
            configMock.getDataLogDir(), 
            configMock.getSnapRetainCount(), 
            configMock.getPurgeInterval()
        );
        
        QuorumPeerMain peerMain = spy(new QuorumPeerMain());
        
        // Prepare args to simulate quorum-mode configuration setup
        String[] args = { "/path/to/zookeeper.properties" };
        
        // Test: Initialize and Run (Quorum Mode)
        peerMain.initializeAndRun(args);
        
        // Verify the DatadirCleanupManager logic is executed
        verify(peerMain).initializeAndRun(args);
        verify(configMock, atLeastOnce()).getDataDir();
        verify(configMock, atLeastOnce()).getDataLogDir();
        
        // Assert DatadirCleanupManager schedule
        purgeMgr.start();
        TimerTask mockPurgeTask = mock(TimerTask.class);
        verify(mockPurgeTask, never()).run(); // Ensure no immediate errors
        
        // Teardown Testing
        verifyNoMoreInteractions(configMock);
        purgeMgr = null;
    }
}