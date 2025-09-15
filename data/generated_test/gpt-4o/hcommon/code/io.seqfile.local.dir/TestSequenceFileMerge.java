package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Test;

import java.io.IOException;

public class TestSequenceFileMerge {
    // Test the SequenceFile merge() method's interaction with the configuration "io.seqfile.local.dir".
    @Test
    public void testMergeFunctionWithValidConfiguration() throws IOException {
    
        Configuration conf = new Configuration();
        conf.set("io.seqfile.local.dir", "file:///tmp"); // Use 'file://' for local directory compatibility
        conf.setStrings(
            "io.serializations",
            JavaSerialization.class.getName(),
            WritableSerialization.class.getName()
        ); // Ensure Serializer is properly configured
        String localDirPropertyValue = conf.get("io.seqfile.local.dir");


        LocalDirAllocator lDirAlloc = new LocalDirAllocator("io.seqfile.local.dir");
        Path tmpDir = new Path(localDirPropertyValue);
        Path tmpFilename = new Path(tmpDir, "intermediate");
        long approxOutputSize = 1024L * 1024 * 10; // Approximate file size

        Path outputFile = lDirAlloc.getLocalPathForWrite(tmpFilename.toString(), approxOutputSize, conf);
        if (outputFile == null) {
            throw new AssertionError("Failed to allocate a valid output path.");
        }

        SequenceFile.Writer writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(outputFile),
                SequenceFile.Writer.keyClass(org.apache.hadoop.io.Text.class), 
                SequenceFile.Writer.valueClass(org.apache.hadoop.io.Text.class) 
        );

        // Write mock key-value pairs to the file
        writer.append(new org.apache.hadoop.io.Text("key1"), new org.apache.hadoop.io.Text("value1"));
        writer.append(new org.apache.hadoop.io.Text("key2"), new org.apache.hadoop.io.Text("value2"));
        writer.sync(); // Ensure data consistency
        writer.close();

        Path parentDirectory = outputFile.getParent();
        if (parentDirectory == null) {
            throw new AssertionError("Parent directory is null.");
        }

        // Correct comparison by removing "file:" prefix for consistency
        String normalizedParentDirectory = parentDirectory.toString().replace("file:", "");
        String normalizedLocalDir = localDirPropertyValue.replace("file://", "");
        if (!normalizedParentDirectory.equals(normalizedLocalDir)) {
            throw new AssertionError("Parent directory does not match the expected local directory. Expected: " + normalizedLocalDir + ", Actual: " + normalizedParentDirectory);
        }
    }
}