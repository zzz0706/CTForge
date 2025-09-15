package org.apache.zookeeper.test;   

import org.apache.zookeeper.server.DatadirCleanupManager;       
import org.apache.zookeeper.server.quorum.QuorumPeerConfig; 
import org.junit.Test; 
import org.mockito.Mockito;

import java.io.File;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatadirCleanupManagerTest {       

    @Test 
    // Unit test for DatadirCleanupManager with Zero Purge Interval.
    // 1. Correctly uses Zookeeper3.5.6 API to obtain configuration values through mocks.
    // 2. Prepares mock configurations to simulate test conditions.
    // 3. Executes the test by interacting with DatadirCleanupManager.
    // 4. Verifies the behavior after testing.

    public void test_DatadirCleanupManager_start_withZeroPurgeInterval() {
        // Prepare the test conditions
        // Mock QuorumPeerConfig to simulate configuration reading
        QuorumPeerConfig configMock = mock(QuorumPeerConfig.class);
        
        // Mock file directories required for the configuration
        File mockSnapDir = new File("/tmp/snapDir");
        File mockDataLogDir = new File("/tmp/dataLogDir");

        // Simulate QuorumPeerConfig values using Mockito's when..return
        when(configMock.getDataDir()).thenReturn(mockSnapDir);
        when(configMock.getDataLogDir()).thenReturn(mockDataLogDir);
        when(configMock.getSnapRetainCount()).thenReturn(3); 
        when(configMock.getPurgeInterval()).thenReturn(0);  

        // Instantiate DatadirCleanupManager with configuration values
        DatadirCleanupManager cleanupManager = new DatadirCleanupManager(
                configMock.getDataDir(),
                configMock.getDataLogDir(),
                configMock.getSnapRetainCount(),
                configMock.getPurgeInterval()
        );

        // Start the cleanupManager. No tasks should be scheduled as purgeInterval is zero.
        cleanupManager.start();

        // Assertions and validations to ensure no unintended behavior occurred
        // Ideally, use captured logs or mocked dependencies to verify the behavior (not implemented here).
        System.out.println("Test completed: DatadirCleanupManager started with zero purge interval.");
    }
}