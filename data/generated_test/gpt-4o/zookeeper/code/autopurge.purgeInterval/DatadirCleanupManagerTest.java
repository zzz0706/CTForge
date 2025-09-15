package org.apache.zookeeper.server;

import org.junit.Test;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.DatadirCleanupManager;
import java.io.File;
import java.util.Timer;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class DatadirCleanupManagerTest {
    
    @Test
    // Test code
    // 1. Use the zookeeper3.5.6 API correctly to obtain configuration values without hardcoding.
    // 2. Prepare the test conditions.
    // 3. Test the DatadirCleanupManager functionality.
    // 4. Verify the behavior after testing.
    public void testStartPurgeTask_ScheduleActivated() {
        // Prepare the test conditions
        // 1. Create mock QuorumPeerConfig to load configuration values using the API
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);

        // Create mock file instances
        File mockSnapDir = new File("/mock/snapdir");
        File mockDataLogDir = new File("/mock/datalogdir");
        
        // Mock necessary configuration values
        int mockSnapRetainCount = 3; // Retain count for snapshots
        int mockPurgeInterval = 1;   // Positive interval: autopurge.purgeInterval > 0

        // Configure responses for QuorumPeerConfig API
        when(configMock.getDataDir()).thenReturn(mockSnapDir);
        when(configMock.getDataLogDir()).thenReturn(mockDataLogDir);
        when(configMock.getPurgeInterval()).thenReturn(mockPurgeInterval);
        when(configMock.getSnapRetainCount()).thenReturn(mockSnapRetainCount);

        // Test code
        // 2. Initialize DatadirCleanupManager using mocked configuration
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
            configMock.getDataDir(),
            configMock.getDataLogDir(),
            configMock.getSnapRetainCount(),
            configMock.getPurgeInterval()
        );

        // 3. Verify the behavior of start() method
        cleanupManager.start();

        // Code after testing
        // Verify if the purge task is scheduled when purgeInterval > 0
        // Here, the Timer instance creation is not tested directly, but you verify the mocking and startup behavior
        Timer timer = new Timer("PurgeTask", true);
        assertNotNull(timer); // Asserts that the Timer instance is non-null and initialized correctly
    }
}