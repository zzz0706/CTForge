package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Test;

import java.io.File;
import java.util.Timer;

import static org.mockito.Mockito.*;

public class DatadirCleanupManagerTest {

    @Test
    // Test code:
    // 1. You need to use the ZooKeeper 3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_start_purgeTaskScheduled() {
        // Step 1: Use ZooKeeper API to fetch configuration values correctly
        File dataDir = mock(File.class); // Mocked data directory
        File dataLogDir = mock(File.class); // Mocked log directory

        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        when(configMock.getDataDir()).thenReturn(dataDir);
        when(configMock.getDataLogDir()).thenReturn(dataLogDir);

        // Retain count and interval values
        int snapRetainCount = 3; // This can be configured based on test requirements
        int purgeInterval = 1;  // Example positive value in hours

        // Step 2: Prepare the test setup
        // Create DatadirCleanupManager object with mocked configuration
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
            configMock.getDataLogDir(),
            configMock.getDataDir(),
            snapRetainCount,
            purgeInterval
        );

        // Mocking Timer as it interacts with schedule operations
        Timer mockTimer = mock(Timer.class);

        // Spy the DatadirCleanupManager to override behavior of non-mocked methods
        DatadirCleanupManager cleanupManagerSpy = spy(cleanupManager);

        // Step 3: Inject the mocked Timer into the spy object using reflection
        try {
            java.lang.reflect.Field timerField = DatadirCleanupManager.class.getDeclaredField("timer");
            timerField.setAccessible(true);
            timerField.set(cleanupManagerSpy, mockTimer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to inject mock Timer into DatadirCleanupManager", e);
        }

        // Act: Start the purge task
        cleanupManagerSpy.start();

        // Step 4: Verify behavior
        // Ensure `start()` was called once
        verify(cleanupManagerSpy, times(1)).start();

        // Ensure Timer's `schedule` method was called with correct parameters
        verify(mockTimer, times(1)).schedule(any(), eq(0L), eq(purgeInterval * 3600L * 1000L));
    }
}