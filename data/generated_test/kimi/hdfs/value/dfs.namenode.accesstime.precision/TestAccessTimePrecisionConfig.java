package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestAccessTimePrecisionConfig {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @After
    public void tearDown() {
        conf.clear();
    }

    @Test
    public void testAccessTimePrecisionValidZero() {
        // 1. Obtain configuration value
        long precision = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
                DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

        // 2. Prepare test conditions: value can be 0 (disables access times)
        // 3. Test code
        assertTrue("Access time precision must be >= 0", precision >= 0);
    }

    @Test
    public void testAccessTimePrecisionValidPositive() {
        // 1. Obtain configuration value
        long precision = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
                DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

        // 2. Prepare test conditions: value must be non-negative long
        // 3. Test code
        assertTrue("Access time precision must be non-negative", precision >= 0);
    }

    @Test
    public void testAccessTimePrecisionTypeCheck() {
        // 1. Obtain configuration value
        String precisionStr = conf.get(
                DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
                String.valueOf(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT));

        // 2. Prepare test conditions: must be parseable as long
        // 3. Test code
        try {
            Long.parseLong(precisionStr);
        } catch (NumberFormatException e) {
            fail("Access time precision must be a valid long integer");
        }
    }
}