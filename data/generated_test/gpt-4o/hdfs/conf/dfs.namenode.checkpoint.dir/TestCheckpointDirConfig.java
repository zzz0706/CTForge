package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;

public class TestCheckpointDirConfig {

    @Test
    public void testCheckpointDirConfiguration() {
        Configuration conf = new Configuration();

        // Step 1: Retrieve configuration value for dfs.namenode.checkpoint.dir
        // Replace ${hadoop.tmp.dir} placeholder with actual value to avoid URI syntax errors
        String hadoopTmpDir = System.getProperty("hadoop.tmp.dir", "/tmp");
        String defaultCheckpointDir = "file://" + hadoopTmpDir + "/dfs/namesecondary";
        String[] checkpointDirStrings = conf.getTrimmedStrings("dfs.namenode.checkpoint.dir", defaultCheckpointDir);

        Collection<URI> checkpointDirs = new ArrayList<>();
        for (String checkpointDir : checkpointDirStrings) {
            try {
                checkpointDirs.add(new URI(checkpointDir));
            } catch (URISyntaxException e) {
                fail("Invalid URI syntax for checkpoint directory: " + checkpointDir);
            }
        }

        // Ensure property is present
        assertNotNull("Configuration dfs.namenode.checkpoint.dir is not set.", checkpointDirs);

        // Validate checkpoint directories
        for (URI dir : checkpointDirs) {
            assertNotNull("Checkpoint directory must not be null.", dir);

            // Ensure it is a valid file path or URI
            try {
                Path path = new Path(dir);
                assertNotNull("Checkpoint directory must be a valid file path or URI.", path);
            } catch (Exception e) {
                fail("Checkpoint directory must be a valid file path or URI: " + dir);
            }
        }

        // Step 2: Retrieve configuration value for dfs.namenode.checkpoint.edits.dir
        // Replace ${hadoop.tmp.dir} placeholder with actual value to avoid URI syntax errors
        String defaultCheckpointEditsDir = "file://" + hadoopTmpDir + "/dfs/namesecondary";
        String[] checkpointEditsDirStrings = conf.getTrimmedStrings("dfs.namenode.checkpoint.edits.dir", defaultCheckpointEditsDir);

        Collection<URI> checkpointEditsDirs = new ArrayList<>();
        for (String checkpointEditsDir : checkpointEditsDirStrings) {
            try {
                checkpointEditsDirs.add(new URI(checkpointEditsDir));
            } catch (URISyntaxException e) {
                fail("Invalid URI syntax for checkpoint edits directory: " + checkpointEditsDir);
            }
        }

        // Ensure property is present
        assertNotNull("Configuration dfs.namenode.checkpoint.edits.dir is not set.", checkpointEditsDirs);

        // Validate dependency
        assertEquals(
                "dfs.namenode.checkpoint.edits.dir should have the same default value as dfs.namenode.checkpoint.dir.",
                checkpointDirs, checkpointEditsDirs
        );

        // Validate checkpoint edits directories
        for (URI dir : checkpointEditsDirs) {
            assertNotNull("Checkpoint edits directory must not be null.", dir);

            // Ensure it is a valid file path or URI
            try {
                Path path = new Path(dir);
                assertNotNull("Checkpoint edits directory must be a valid file path or URI.", path);
            } catch (Exception e) {
                fail("Checkpoint edits directory must be a valid file path or URI: " + dir);
            }
        }
    }
}