package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;

// Test class for BloomMapFile.Writer
public class TestBloomMapFileWriter {

    private java.nio.file.Path tempDir; // Temporary directory for testing
    private Configuration configuration;

    // Prepare the test conditions before each test
    @Before
    public void setUp() throws IOException {
        // Create a temporary directory for simulated file operations
        tempDir = Files.createTempDirectory("testWriterInitialization");

        // Initialize Hadoop Configuration and use API to set necessary configurations
        configuration = new Configuration();
        configuration.setClass("io.serializations", WritableSerialization.class, Object.class);
        configuration.setClass("bloom.mapfile.keyclass", Text.class, Object.class);
        configuration.set("bloom.mapfile.comparator", Text.Comparator.class.getName());
    }

    // Test initialization of BloomMapFile.Writer with a valid configuration
    @Test
    public void testWriterInitializationWithValidConfiguration() throws IOException {
        BloomMapFile.Writer writer = null;
        try {
            // Obtain the FileSystem instance using the configuration
            FileSystem fs = FileSystem.get(configuration);
            
            // Create a Path instance for the temporary directory
            Path path = new Path(tempDir.toUri());
            
            // Create an instance of BloomMapFile.Writer using the provided Configuration and Path
            writer = new BloomMapFile.Writer(
                configuration,
                fs,
                path.toString(),
                Text.class,
                Text.class
            );

            // Verify that the writer instance is not null
            assertNotNull("BloomMapFile Writer should not be null after initialization", writer);
        } finally {
            if (writer != null) {
                // Clean up resources
                writer.close();
            }
        }
    }

    // Additional helper method for assertions
    private void assertNotNull(String message, Object object) {
        if (object == null) {
            throw new AssertionError(message);
        }
    }
}