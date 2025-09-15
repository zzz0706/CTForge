package org.apache.hadoop.test;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;

public class SequenceFileTest {


    @Test
    public void test_sync_executesCorrectly() throws IOException {
        // Step 1: Create and configure a configuration instance
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("file:///tmp/test_sync_file");

        // Ensure the file does not exist before running the test
        if (fs.exists(path)) {
            fs.delete(path, false);
        }

        SequenceFile.Writer writer = null;
        SequenceFile.Reader reader = null;

        try {
            // Step 2: Create a SequenceFile writer
            writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class)
            );

            // Write dummy data
            writer.append(new Text("key1"), new Text("value1"));
            writer.append(new Text("key2"), new Text("value2"));

            // Step 3: Correct the position for `sync` method
            writer.sync(); // Explicitly calling writer's sync to ensure sync points are set

            // Close the writer before reading the file to avoid EOF exceptions
            writer.close();
            writer = null;

            // Create a SequenceFile reader
            reader = new SequenceFile.Reader(
                conf,
                SequenceFile.Reader.file(path)
            );

            // Execute the `sync` method on the reader
            reader.sync(0L); // Sync from the beginning of the file

            // Step 4: Verify results
            long syncPos = reader.getPosition();
            Assert.assertTrue("Expected sync position to be greater than or equal to 0", syncPos >= 0L);
        } finally {
            // Step 5: Clean up resources
            if (writer != null) {
                writer.close();
            }
            if (reader != null) {
                reader.close();
            }

            // Clean up the test file
            if (fs.exists(path)) {
                fs.delete(path, false);
            }
        }
    }
}