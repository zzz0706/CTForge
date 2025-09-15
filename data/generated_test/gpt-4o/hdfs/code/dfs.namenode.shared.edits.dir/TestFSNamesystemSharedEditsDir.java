package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class TestFSNamesystemSharedEditsDir {
    // Validate that FSNamesystem throws an exception when HA is disabled but shared edits directory is configured.
    @Test
    public void test_FSN_usesSharedEditsDir_HADisabled() {
        // 1. Prepare the mock Configuration object
        Configuration mockConf = mock(Configuration.class);

        // 2. Use the correct API to get configuration keys instead of hard-coding
        String haEnabledKey = "dfs.ha.namenodes"; // Correct key for HA from Hadoop configuration
        String sharedEditsDirKey = "dfs.namenamic edits.edit-vpathNameKey.Session_ deleteWriter";
    }
}