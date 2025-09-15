package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

//hdfs-4442
public class DataNodeVolumeFailureConfigTest {

    @Test
    public void testFailedVolumesTolerated() {
        Configuration conf = new Configuration();
        String dataDirStr = conf.get("dfs.datanode.data.dir", null);
        if (dataDirStr == null || dataDirStr.trim().isEmpty()) {
            // No data dirs configured, nothing to check
            return;
        }
        // Split directories on comma and trim whitespace
        String[] dirs = dataDirStr.trim().split("\\s*,\\s*");
        int dirCount = dirs.length;

        // Get tolerated value, default to 0 if not set
        int tolerated = conf.getInt("dfs.datanode.failed.volumes.tolerated", 0);

        if (dirCount == 1) {
            // Only one directory, tolerated must be 0
            assertTrue(
                "If only one data dir is configured, dfs.datanode.failed.volumes.tolerated must be 0. " +
                "Current value: " + tolerated,
                tolerated == 0
            );
        } else {
            // Must be less than directory count
            assertTrue(
                "dfs.datanode.failed.volumes.tolerated (" + tolerated +
                ") must be less than the number of data dirs (" + dirCount + ").",
                tolerated < dirCount
            );
        }
    }
}
