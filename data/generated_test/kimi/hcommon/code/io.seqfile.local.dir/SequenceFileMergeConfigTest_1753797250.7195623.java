package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class SequenceFileMergeConfigTest {

    @Test
    public void nonExistentDirsAreIgnored() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        File dir1 = new File("target/test/io1");
        File dir2 = new File("target/test/io2");
        dir1.mkdirs();
        dir2.mkdirs();

        String existingDir1 = dir1.getAbsolutePath();
        String nonExistentDir = "/nonexistent/io";
        String existingDir2 = dir2.getAbsolutePath();

        conf.set("io.seqfile.local.dir", String.join(",", existingDir1, nonExistentDir, existingDir2));

        // 3. Test code.
        LocalDirAllocator allocator = new LocalDirAllocator("io.seqfile.local.dir");
        Path selectedPath = allocator.getLocalPathForWrite("testfile", 1024, conf);

        // 4. Code after testing.
        String selectedDir = selectedPath.getParent().toString();
        assertTrue("Selected directory should be one of the existing ones",
                selectedDir.equals(existingDir1) || selectedDir.equals(existingDir2));
        assertTrue("Non-existent directory should not be used",
                !selectedDir.equals(nonExistentDir));
    }
}