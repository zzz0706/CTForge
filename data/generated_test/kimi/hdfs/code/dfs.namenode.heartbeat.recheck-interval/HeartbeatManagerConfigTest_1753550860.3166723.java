package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.StopWatch;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class HeartbeatManagerConfigTest {

    @Mock
    private Namesystem namesystem;
    
    @Mock
    private BlockManager blockManager;
    
    private Configuration conf;
    
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
    }

    @Test
    public void testShouldAbortHeartbeatCheckBasedOnRecheckInterval() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Create a Configuration instance and set dfs.namenode.heartbeat.recheck-interval to a test value (e.g., 5000).
        long testRecheckInterval = 5000L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, testRecheckInterval);
        
        // 2. Prepare the test conditions.
        // Instantiate HeartbeatManager with the Configuration.
        when(namesystem.isRunning()).thenReturn(false); // Stop immediately to avoid running the thread
        HeartbeatManager heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
        
        // Access private heartbeatStopWatch field to reset it
        java.lang.reflect.Field stopWatchField = HeartbeatManager.class.getDeclaredField("heartbeatStopWatch");
        stopWatchField.setAccessible(true);
        StopWatch stopWatch = (StopWatch) stopWatchField.get(heartbeatManager);
        
        // Call restartHeartbeatStopWatch() to start the timer.
        heartbeatManager.restartHeartbeatStopWatch();
        long startTime = Time.monotonicNow();
        
        // 3. Test code.
        // Wait for a duration less than the recheck interval (e.g., 3000 ms).
        Thread.sleep(3000L);
        
        // Call shouldAbortHeartbeatCheck(0).
        boolean result1 = heartbeatManager.shouldAbortHeartbeatCheck(0);
        
        // Assert that it returns false.
        assertFalse("Should not abort when elapsed time is less than recheck interval", result1);
        
        // Wait for a total duration that exceeds the recheck interval (e.g., 3000 more ms, total 6000 ms).
        Thread.sleep(3000L);
        long totalTime = Time.monotonicNow() - startTime;
        
        // Call shouldAbortHeartbeatCheck(0).
        boolean result2 = heartbeatManager.shouldAbortHeartbeatCheck(0);
        
        // 4. Code after testing.
        // Assert that it returns true.
        assertTrue("Should abort when elapsed time exceeds recheck interval", result2);
        
        // Also test with offset
        boolean result3 = heartbeatManager.shouldAbortHeartbeatCheck(-1000L);
        assertFalse("Should not abort when elapsed time plus negative offset is less than recheck interval", result3);
        
        boolean result4 = heartbeatManager.shouldAbortHeartbeatCheck(1000L);
        assertTrue("Should abort when elapsed time plus positive offset exceeds recheck interval", result4);
    }
}