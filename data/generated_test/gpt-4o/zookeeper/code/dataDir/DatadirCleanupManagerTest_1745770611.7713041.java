package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.QuorumPeerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;

import java.io.File;
import java.util.Timer;

import static org.mockito.Mockito.*;

public class DatadirCleanupManagerTest {

    @Test
    // Test code:
    // 1. You need to use the ZooKeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test the behavior and functionality of DatadirCleanupManager's `start` method and related configurations.
    // 4. Verify results and clean up after testing.
    public void test_start_purgeTaskScheduled() {
        // Step 1: Use ZooKeeper API to fetch configuration values correctly
        File dataDir = mock(File.class); // Mocked snapshot directory
        File dataLogDir = mock(File.class); // Mocked transaction log directory

        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.getDataDir()).thenReturn(dataDir);
        when(configMock.getDataLogDir()).thenReturn(dataLogDir);
        when(configMock.getSnapRetainCount()).thenReturn(3); // Retain 3 snapshots
        when(configMock.getPurgeInterval()).thenReturn(1); // Purge interval: 1 hour

        // Step 2: Prepare the test setup
        // Create the DatadirCleanupManager instance based on the mocked configuration
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
            configMock.getDataLogDir(),
            configMock.getDataDir(),
            configMock.getSnapRetainCount(),
            configMock.getPurgeInterval()
        );

        // Mock and override Timer behavior
        Timer mockTimer = mock(Timer.class);
        
        DatadirCleanupManager cleanupManagerSpy = spy(cleanupManager);

        // Use reflection to inject the mocked Timer into the spy object
        try {
            java.lang.reflect.Field timerField = DatadirCleanupManager.class.getDeclaredField("timer");
            timerField.setAccessible(true);
            timerField.set(cleanupManagerSpy, mockTimer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to inject mock Timer into DatadirCleanupManager", e);
        }

        // Step 3: Act - Invoke the `start` method to trigger the purge task scheduling
        cleanupManagerSpy.start();

        // Step 4: Verify the behavior
        // Verify that `start` was invoked once
        verify(cleanupManagerSpy, times(1)).start();

        // Verify that Timer's `schedule` method was invoked with the correct parameters
        verify(mockTimer, times(1)).schedule(
            any(), 
            eq(0L), 
            eq(configMock.getPurgeInterval() * TimeUnit.HOURS.toMillis(configMock.getPurgeInterval()))
        );
    }
}