package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static org.junit.Assert.*;

//HBASE-22701
@Category(SmallTests.class)
public class TestHBaseLocalDirConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestHBaseLocalDirConfig.class);

    @Test
    public void testHBaseLocalDirConfig() {
        Configuration conf = new Configuration();
        String localDirPath = conf.get("hbase.local.dir", "").trim();

        // Allow unset (empty), which means default value will be used
        if (localDirPath.isEmpty())
            return;

        File localDir = new File(localDirPath);

        assertTrue(
                "hbase.local.dir does not exist: " + localDirPath,
                localDir.exists());
        assertTrue(
                "hbase.local.dir is not a directory: " + localDirPath,
                localDir.isDirectory());
        assertTrue(
                "hbase.local.dir is not writable: " + localDirPath,
                localDir.canWrite());
    }
}
