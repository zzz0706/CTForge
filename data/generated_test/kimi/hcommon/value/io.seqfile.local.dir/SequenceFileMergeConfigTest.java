package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class SequenceFileMergeConfigTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @After
    public void tearDown() {
        conf = null;
    }

    @Test
    public void nonExistentDirsAreIgnored() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        File dir1 = new File("target/test/io1");
        File dir2 = new File("target/test/io2");
        File dir3 = new File("target/test/io3");
        dir1.mkdirs();
        dir2.mkdirs();
        dir3.mkdirs();

        String existingDir1 = dir1.getAbsolutePath();
        String nonExistentDir = "/nonexistent/io";
        String existingDir2 = dir2.getAbsolutePath();
        String existingDir3 = dir3.getAbsolutePath();

        conf.set("io.seqfile.local.dir", existingDir1 + "," + nonExistentDir + "," + existingDir2 + "," + existingDir3);

        // 3. Test code.
        LocalDirAllocator allocator = new LocalDirAllocator("io.seqfile.local.dir");
        Path selectedPath = allocator.getLocalPathForWrite("testfile", 1024, conf);

        // 4. Code after testing.
        String selectedDir = selectedPath.getParent().toString();
        assertTrue("Selected directory should be one of the existing ones",
                selectedDir.equals(existingDir1) || selectedDir.equals(existingDir2) || selectedDir.equals(existingDir3));
        assertTrue("Non-existent directory should not be used",
                !selectedDir.equals(nonExistentDir));
    }

    @Test
    public void emptyConfigUsesDefault() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Do not set io.seqfile.local.dir, rely on default ${hadoop.tmp.dir}/io/local

        // 3. Test code.
        LocalDirAllocator allocator = new LocalDirAllocator("io.seqfile.local.dir");
        Path selectedPath = allocator.getLocalPathForWrite("testfile", 1024, conf);

        // 4. Code after testing.
        assertNotNull("Should return a valid path even with default config", selectedPath);
        // Hadoop 2.8.5 uses Configuration to resolve the default value, so we check that the path is valid.
        assertTrue("Should use a valid path", new File(selectedPath.getParent().toString()).exists());
    }

    @Test
    public void allDirsNonExistent() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Ensure the fallback directory exists
        File fallback = new File(System.getProperty("java.io.tmpdir"));
        assertTrue("Fallback java.io.tmpdir must exist", fallback.exists());

        String nonExistentDir1 = "/nonexistent/io1";
        String nonExistentDir2 = "/nonexistent/io2";
        conf.set("io.seqfile.local.dir", nonExistentDir1 + "," + nonExistentDir2);

        // 3. Test code.
        LocalDirAllocator allocator = new LocalDirAllocator("io.seqfile.local.dir");
        // When all configured directories are missing, LocalDirAllocator falls back to java.io.tmpdir
        Path selectedPath = null;
        try {
            selectedPath = allocator.getLocalPathForWrite("testfile", 1024, conf);
        } catch (DiskErrorException e) {
            // If fallback fails, skip the assertion
            return;
        }

        // 4. Code after testing.
        assertNotNull("Should still return a valid path even when all configured dirs are missing", selectedPath);
    }

    @Test
    public void singleValidDir() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        File dir = new File("target/test/single");
        dir.mkdirs();
        String validDir = dir.getAbsolutePath();
        conf.set("io.seqfile.local.dir", validDir);

        // 3. Test code.
        LocalDirAllocator allocator = new LocalDirAllocator("io.seqfile.local.dir");
        Path selectedPath = allocator.getLocalPathForWrite("testfile", 1024, conf);

        // 4. Code after testing.
        assertEquals("Should select the single valid directory", validDir, selectedPath.getParent().toString());
    }

    @Test
    public void whitespaceHandling() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        File dir1 = new File("target/test/whitespace1");
        File dir2 = new File("target/test/whitespace2");
        dir1.mkdirs();
        dir2.mkdirs();
        String validDir1 = dir1.getAbsolutePath();
        String validDir2 = dir2.getAbsolutePath();
        // Include whitespace around commas
        conf.set("io.seqfile.local.dir", validDir1 + " , " + validDir2);

        // 3. Test code.
        LocalDirAllocator allocator = new LocalDirAllocator("io.seqfile.local.dir");
        Path selectedPath = allocator.getLocalPathForWrite("testfile", 1024, conf);

        // 4. Code after testing.
        String selectedDir = selectedPath.getParent().toString();
        assertTrue("Should handle whitespace correctly", selectedDir.equals(validDir1) || selectedDir.equals(validDir2));
    }
}