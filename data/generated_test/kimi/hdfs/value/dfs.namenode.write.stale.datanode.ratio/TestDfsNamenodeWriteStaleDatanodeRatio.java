package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestDfsNamenodeWriteStaleDatanodeRatio {

    private Configuration conf;

    @Before
    public void setUp() {
        // 1. Prepare the test conditions: create a fresh Configuration object
        conf = new Configuration();
    }

    @Test
    public void testValidRatioUseStaleDataNodesForWrite() {
        // 2. Test valid range: 0 < value <= 1.0
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, 0.5f);
        float ratio = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT);
        assertTrue("Ratio must be > 0", ratio > 0);
        assertTrue("Ratio must be <= 1.0", ratio <= 1.0f);
    }

    @Test
    public void testInvalidRatioZero() throws IOException {
        // 3. Test invalid value: zero
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, 0.0f);
        try {
            new DatanodeManager(null, null, conf);
            fail("Expected IllegalArgumentException for zero ratio");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("is invalid"));
        }
    }

    @Test
    public void testInvalidRatioGreaterThanOne() throws IOException {
        // 4. Test invalid value: > 1.0
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, 1.1f);
        try {
            new DatanodeManager(null, null, conf);
            fail("Expected IllegalArgumentException for ratio > 1.0");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("is invalid"));
        }
    }

    @Test
    public void testInvalidRatioNegative() throws IOException {
        // 5. Test invalid value: negative
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY, -0.5f);
        try {
            new DatanodeManager(null, null, conf);
            fail("Expected IllegalArgumentException for negative ratio");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("is invalid"));
        }
    }

    @After
    public void tearDown() {
        // 6. Code after testing: clean up
        conf = null;
    }
}