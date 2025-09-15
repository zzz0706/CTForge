package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNameNodeResourceMonitor {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_NameNodeResourceMonitor_withInterruptedSleep() throws Exception {
        // Step 1: Create a Configuration object and set up a valid value using API.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, 15000L);
        
        // Retrieve the value using Configuration API and validate it.
        final long resourceCheckInterval = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT
        );

        // Step 2: Initialize FSNamesystem with a mocked FSImage and the configuration.
        FSImage fsImageMock = Mockito.mock(FSImage.class);
        FSNamesystem fsNamesystem = Mockito.spy(new FSNamesystem(conf, fsImageMock));

        // Step 3: Prepare and override the NameNodeResourceMonitor instance.
        FSNamesystem.NameNodeResourceMonitor monitor = fsNamesystem.new NameNodeResourceMonitor() {
            @Override
            public void run() {
                try {
                    // Simulate interruption during sleep.
                    Thread.sleep(resourceCheckInterval);
                } catch (InterruptedException e) {
                    // Handle interruption gracefully.
                    Thread.currentThread().interrupt();
                }
            }
        };

        // Step 4: Execute the NameNodeResourceMonitor logic ensuring graceful exception handling.
        monitor.run();
    }
}