package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;

public class TestSequenceFileCompression {

    
    @Test
    public void testAppendTriggersBlockCompressionOnThreshold() throws IOException {
        // Step 1: Create a Configuration object and retrieve the block size value using the API.
        Configuration conf = new Configuration();
        int compressionBlockSize = conf.getInt(
            "io.seqfile.compress.blocksize", // The key for block compression size
            1000000 // Default block size
        );

        // Step 2: Prepare a SequenceFile.Writer options to configure the writer for testing.
        File tempFile = File.createTempFile("test-seqfile", "seq");
        tempFile.deleteOnExit(); // Ensure the file is deleted after the test finishes
        Path tempFilePath = new Path(tempFile.getAbsolutePath()); // Convert java.io.File to org.apache.hadoop.fs.Path

        Writer.Option fileOption = Writer.file(tempFilePath);
        Writer.Option keyClassOption = Writer.keyClass(Text.class);
        Writer.Option valueClassOption = Writer.valueClass(Text.class);
        Writer.Option compressionOption = Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec());
        Writer.Option blockSizeOption = Writer.bufferSize(compressionBlockSize);

        // Step 3: Create a spy object for SequenceFile.Writer to track method interactions.
        SequenceFile.Writer realWriter = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption, compressionOption, blockSizeOption);
        SequenceFile.Writer writer = spy(realWriter);

        // Step 4: Prepare mock key and value instances matching the expected serialization types.
        Text mockKey = new Text("mockKey");
        Text mockValue = new Text("mockValue");

        // Step 5: Append enough key-value pairs to exceed the compression block size threshold.
        int recordSize = mockKey.getLength() + mockValue.getLength();
        int numberOfRecords = (compressionBlockSize / recordSize) + 1;
        for (int i = 0; i < numberOfRecords; i++) {
            writer.append(mockKey, mockValue);
        }

        // Step 6: Close the writer to ensure proper handling and syncing of data.
        writer.close();

        // Step 7: Verify that the `sync()` method is called as expected.
        verify(writer, atLeastOnce()).sync();
    }
}