package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class FSDirectoryTest {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_createReservedStatuses() throws Exception {
        // Step 1: Prepare the configuration
        Configuration conf = new Configuration();
        conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true); // Enable permissions
        conf.set(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY, "supergroup"); // Set superuser group to "supergroup"
        String superGroup = conf.get(DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY, "supergroup");

        // Step 2: Initialize FSImage and FSNamesystem correctly
        FSImage fsImage = new FSImage(conf);
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);

        FSDirectory fsDirectory = fsNamesystem.getFSDirectory();

        // Step 3: Invoke `createReservedStatuses` method and prepare testing environment
        long cTime = System.currentTimeMillis();
        fsDirectory.createReservedStatuses(cTime);

        // Step 4: Retrieve and validate the reserved statuses
        HdfsFileStatus[] reservedStatuses = fsDirectory.getReservedStatuses();
        assertNotNull("Reserved statuses should not be null", reservedStatuses);
        assertEquals("Expected 2 reserved statuses", 2, reservedStatuses.length);

        for (HdfsFileStatus status : reservedStatuses) {
            assertEquals("Group of reserved path should be the superuser group", superGroup, status.getGroup());
            assertEquals("Reserved path should have permissions 0770", new FsPermission((short) 0770), status.getPermission());
        }
    }
}