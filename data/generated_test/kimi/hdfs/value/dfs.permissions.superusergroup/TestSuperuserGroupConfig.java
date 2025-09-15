package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSuperuserGroupConfig {

    private Configuration conf;

    @Before
    public void setUp() {
        // 1. Obtain configuration values via the HDFS 2.8.5 API
        conf = new HdfsConfiguration();
    }

    @Test
    public void testSuperuserGroupIsSingleGroupName() {
        // 2. Prepare test conditions – no value set in code; rely on conf files
        String supergroup = conf.get(
                DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
                DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);

        // 3. Test code – verify it is exactly one group name
        assertNotNull("dfs.permissions.superusergroup must not be null", supergroup);
        assertFalse("dfs.permissions.superusergroup must not be empty", supergroup.trim().isEmpty());

        // Ensure no comma or whitespace that would indicate multiple groups
        assertFalse("dfs.permissions.superusergroup must be a single group name (no commas)",
                supergroup.contains(","));
        assertEquals("dfs.permissions.superusergroup must not contain leading/trailing whitespace",
                supergroup.trim(), supergroup);
    }

    @After
    public void tearDown() {
        // 4. Code after testing – nothing to clean up
    }
}