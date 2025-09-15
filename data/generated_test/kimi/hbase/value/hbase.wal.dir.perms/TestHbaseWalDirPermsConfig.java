package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestHbaseWalDirPermsConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHbaseWalDirPermsConfig.class);

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testHbaseWalDirPermsValid() throws IOException {
        String permsStr = conf.get("hbase.wal.dir.perms", "700");
        assertTrue("hbase.wal.dir.perms must be a 3-digit octal string",
                permsStr.matches("[0-7]{3}"));
    }

    @Test
    public void testHbaseWalDirPermsInvalid() {
        conf.set("hbase.wal.dir.perms", "999");
        String permsStr = conf.get("hbase.wal.dir.perms");
        assertTrue("hbase.wal.dir.perms must be a 3-digit octal string",
                !permsStr.matches("[0-7]{3}"));
    }

    @Test
    public void testHbaseWalDirPermsEmpty() {
        conf.set("hbase.wal.dir.perms", "");
        String permsStr = conf.get("hbase.wal.dir.perms");
        assertTrue("hbase.wal.dir.perms must be a 3-digit octal string",
                !permsStr.matches("[0-7]{3}"));
    }
}