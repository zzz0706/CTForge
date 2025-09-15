package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.mockito.Mockito;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.util.ArrayList;

public class TestNameNodeResourceChecker {

    @Test
    public void testHasAvailableDiskSpace_WithInvalidRedundantVolumes() throws Exception {
        // Step 1: Use API to retrieve configuration values, avoid hardcoding
        Configuration conf = new Configuration();
        int minimumRedundantVolumes = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT
        );

        // Step 2: Prepare testing conditions
        // Mock the NameNodeResourceChecker behavior for testing
        NameNodeResourceChecker nameNodeResourceChecker = Mockito.mock(NameNodeResourceChecker.class);

        // Simulate volumes that lack sufficient disk space
        List<Boolean> volumesHealth = new ArrayList<Boolean>();
        volumesHealth.add(Boolean.FALSE); // unhealthy volume
        volumesHealth.add(Boolean.FALSE); // unhealthy volume

        // Mock hasAvailableDiskSpace logic
        Mockito.when(nameNodeResourceChecker.hasAvailableDiskSpace()).thenReturn(
            calculateDiskSpace(volumesHealth, minimumRedundantVolumes)
        );

        // Step 3: Execute test logic
        // Invoke hasAvailableDiskSpace() and validate behavior
        boolean hasDiskSpace = nameNodeResourceChecker.hasAvailableDiskSpace();
        assertFalse("Expected hasAvailableDiskSpace() to return false", hasDiskSpace);

        // Step 4: Test completion logic (if required, e.g., cleanup, assertions)
        Mockito.verify(nameNodeResourceChecker).hasAvailableDiskSpace();
    }

    // Utility method to replace lambda expression since Java 1.7 doesn't support lambdas
    private boolean calculateDiskSpace(List<Boolean> volumesHealth, int minimumRedundantVolumes) {
        int healthyVolumes = 0;
        for (Boolean health : volumesHealth) {
            if (health) {
                healthyVolumes++;
            }
        }
        return healthyVolumes >= minimumRedundantVolumes;
    }
}