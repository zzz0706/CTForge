package org.apache.hadoop.io;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.fs.LocalDirAllocator;       
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;       
       
import java.io.File;       
import java.io.IOException;       
       
import static org.junit.Assert.*;       
       
public class TestSequenceFileLocalDirConfig {       
    //test code
    @Test
    public void testIoSeqfileLocalDirConfiguration() {
        // Step 1: Get the configuration value from the Hadoop Configuration
        Configuration conf = new Configuration();
        
        // Configure a property for testing purposes (simulate ${hadoop.tmp.dir})
        String fakeTempDir = System.getProperty("java.io.tmpdir"); // Java temp directory
        conf.set("hadoop.tmp.dir", fakeTempDir);
        
        // Specify a configuration to test against
        conf.set("io.seqfile.local.dir", "${hadoop.tmp.dir}/io/local");
        
        // Step 2: Retrieve and validate the configuration value
        String localDirConfig = conf.get("io.seqfile.local.dir", "${hadoop.tmp.dir}/io/local");
        assertNotNull("io.seqfile.local.dir configuration is null", localDirConfig);
        assertFalse("io.seqfile.local.dir configuration is empty", localDirConfig.isEmpty());

        // Step 3: Use LocalDirAllocator to manage these directories
        LocalDirAllocator dirAllocator = new LocalDirAllocator("io.seqfile.local.dir");

        try {
            // Resolve and validate each directory
            String[] directories = localDirConfig.split(",");
            for (String directory : directories) {
                directory = directory.trim();
                
                // Resolve directory path within the given configuration
                Path resolvedPath = dirAllocator.getLocalPathForWrite(directory, conf);
                FileSystem fs = resolvedPath.getFileSystem(conf);
                File resolvedDir = new File(resolvedPath.toUri().getPath());

                assertNotNull("Resolved directory is null", resolvedDir);

                // Ensure the directory exists or create it for testing purposes
                if (!resolvedDir.exists()) {
                    boolean created = resolvedDir.mkdirs();
                    assertTrue("Failed to create test directory: " + resolvedDir.getAbsolutePath(), created);
                }

                // Ensure the directory is writable
                assertTrue("Directory is not writable: " + resolvedDir.getAbsolutePath(), resolvedDir.canWrite());

                // Ensure the directory is actually a directory
                assertTrue("Path is not a directory: " + resolvedDir.getAbsolutePath(), resolvedDir.isDirectory());

                // Cleanup created directories (optional for a unit test context)
                resolvedDir.deleteOnExit();
            }
        } catch (IOException e) {
            fail("An exception occurred during directory validation: " + e.getMessage());
        }
    }
}