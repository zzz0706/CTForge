package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestNameNodeResourceMonitorExecutionFrequency {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        // Load configuration as the system would
        conf = new Configuration();
        conf.addResource("hdfs-default.xml");
    }

    @Test
    // testResourceMonitorExecutionFrequency
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testResourceMonitorExecutionFrequency() throws Exception {
        // Setup custom configuration value
        long customInterval = 100L; // 100ms interval
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, customInterval);

        // Create FSNamesystem with mocked dependencies
        FSImage fsImage = Mockito.mock(FSImage.class);
        FSNamesystem fsNamesystem = Mockito.spy(new FSNamesystem(conf, fsImage));

        // Mock the methods that would interact with the file system
        doReturn(true).when(fsNamesystem).nameNodeHasResourcesAvailable();
        doNothing().when(fsNamesystem).checkAvailableResources();

        // Access the NameNodeResourceMonitor inner class
        Class<?> innerClass = Class.forName("org.apache.hadoop.hdfs.server.namenode.FSNamesystem$NameNodeResourceMonitor");
        Object resourceMonitor = innerClass.getDeclaredConstructor(FSNamesystem.class).newInstance(fsNamesystem);

        // Set fsRunning to true to allow the monitor to run
        Field fsRunningField = FSNamesystem.class.getDeclaredField("fsRunning");
        fsRunningField.setAccessible(true);
        fsRunningField.set(fsNamesystem, true);

        // Create a thread to run the monitor
        Thread monitorThread = new Thread((Runnable) resourceMonitor);
        monitorThread.setDaemon(true);
        monitorThread.start();

        // Let it run for 500ms
        Thread.sleep(500);

        // Stop the monitor
        Method stopMethod = innerClass.getDeclaredMethod("stopMonitor");
        stopMethod.invoke(resourceMonitor);

        // Wait for the thread to finish
        monitorThread.join(1000);

        // Verify that checkAvailableResources was called approximately 5 times (500ms / 100ms)
        verify(fsNamesystem, atLeast(4)).checkAvailableResources();
        verify(fsNamesystem, atMost(6)).checkAvailableResources();
    }
}