package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
//HDFS-15027
/**
 * Test that each dfs.datanode.data.dir is a valid absolute path,
 * and does not contain any obviously illegal characters for UNIX-like filesystems.
 */
public class DataNodeDataDirPathValidityTest {

    @Test
    public void testDataDirPathValidity() {
        Configuration conf = new Configuration();
        String dataDirs = conf.get("dfs.datanode.data.dir", null);

        if (dataDirs == null || dataDirs.trim().isEmpty()) {
            // No data dirs configured, skip the test.
            return;
        }

        // Typical allowed UNIX path chars: letters, numbers, /, -, _, ., ~, and path separators.
        // You may adjust this pattern according to your policy.
        String validPathPattern = "^[\\w/\\-_.~]+$";

        String[] dirs = dataDirs.split("\\s*,\\s*");
        for (String dir : dirs) {
            dir = dir.trim();
            // Must be an absolute path
            assertTrue(
                "dfs.datanode.data.dir must be an absolute path: " + dir,
                new File(dir).isAbsolute()
            );
            // Path only contains valid chars
            assertTrue(
                "dfs.datanode.data.dir contains illegal characters: " + dir,
                dir.matches(validPathPattern)
            );
        }
    }
}
