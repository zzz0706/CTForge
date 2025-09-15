package org.apache.hadoop.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SequenceFileMergeConfigTest {

    private File tempBase;
    private File disk1Dir;
    private File disk2Dir;
    private File disk3Dir;

    @Before
    public void setUp() throws IOException {
        tempBase = new File(System.getProperty("java.io.tmpdir"), "seqfile-test");
        tempBase.mkdirs();

        disk1Dir = new File(tempBase, "disk1/io");
        disk2Dir = new File(tempBase, "disk2/io");
        disk3Dir = new File(tempBase, "disk3/io");

        disk1Dir.mkdirs();
        disk2Dir.mkdirs();
        disk3Dir.mkdirs();
    }

    @After
    public void tearDown() {
        deleteRecursively(tempBase);
    }

    @Test
    public void customCommaSeparatedDirsAreIterated() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        String customDirs = disk1Dir.getAbsolutePath() + "," +
                            disk2Dir.getAbsolutePath() + "," +
                            disk3Dir.getAbsolutePath();
        conf.set("io.seqfile.local.dir", customDirs);

        // 2. Dynamic expected value calculation
        String expectedDirPrefix = new File(disk2Dir, "intermediate").getAbsolutePath();

        // 3. Prepare the test conditions
        // RawLocalFileSystem initialization is not necessary for LocalDirAllocator

        // 4. Test code
        LocalDirAllocator lDirAlloc = new LocalDirAllocator("io.seqfile.local.dir");
        Path selectedPath = lDirAlloc.getLocalPathForWrite(
                "intermediate.1", 50 * 1024 * 1024, conf);

        // 5. Code after testing
        assertTrue("Selected path should start with disk2 directory",
                   selectedPath.toString().startsWith(disk2Dir.getAbsolutePath()));
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }
}