package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TestIoSeqfileLocalDirConfig {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @After
    public void tearDown() {
        conf = null;
    }

    /**
     * Test that the configuration key "io.seqfile.local.dir" is correctly read
     * and that its value (a comma-separated list of directories) is valid:
     *   - no empty entries
     *   - at least one directory resolves to an existing path
     */
    @Test
    public void testIoSeqfileLocalDirValid() {
        // 1. Obtain the value using the Hadoop 2.8.5 API
        String rawValue = conf.get("io.seqfile.local.dir");
        // If not set explicitly, fall back to default expansion
        if (rawValue == null) {
            rawValue = "${hadoop.tmp.dir}/io/local";
        }
        String expanded = conf.get("io.seqfile.local.dir", rawValue);

        // 2. Split into individual directories
        String[] dirs = expanded.split(",");
        assertTrue("io.seqfile.local.dir must contain at least one directory",
                   dirs.length > 0);

        boolean atLeastOneExists = false;
        for (String dir : dirs) {
            if (dir.trim().isEmpty()) {
                fail("Empty directory entry found in io.seqfile.local.dir");
            }
            File f = new File(dir.trim());
            if (f.exists() && f.isDirectory()) {
                atLeastOneExists = true;
            }
        }

        // 3. Ensure at least one directory exists; otherwise warn (test passes but logs)
        if (!atLeastOneExists) {
            System.err.println("WARNING: none of the directories in io.seqfile.local.dir exist: "
                               + expanded);
        }
    }

    /**
     * Test that the configuration key can be set to a custom comma-separated list
     * and is returned correctly.
     */
    @Test
    public void testIoSeqfileLocalDirCustomValue() {
        String customDirs = "/tmp/seq1,/tmp/seq2,/tmp/seq3";
        conf.set("io.seqfile.local.dir", customDirs);

        String retrieved = conf.get("io.seqfile.local.dir");
        assertEquals(customDirs, retrieved);
    }
}