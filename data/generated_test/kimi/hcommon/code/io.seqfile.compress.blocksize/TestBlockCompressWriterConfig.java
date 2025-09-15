package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestBlockCompressWriterConfig {

    @Test
    public void BlockCompressWriterUsesCustomBlockSizeWhenConfigSet() throws Exception {
        // 1. Prepare Configuration with custom block size and required serializers
        Configuration conf = new Configuration();
        conf.setInt(CommonConfigurationKeysPublic.IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY, 50000);
        // Ensure default Java serialization is available for Long
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");

        // 2. Create minimal dependencies to instantiate BlockCompressWriter
        Path tempPath = new Path("file:///tmp/test.seq");
        FileSystem fs = FileSystem.getLocal(conf);
        SequenceFile.Writer.Option[] options = {
            SequenceFile.Writer.file(tempPath),
            SequenceFile.Writer.keyClass(LongWritable.class),
            SequenceFile.Writer.valueClass(LongWritable.class),
            SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
        };

        // 3. Instantiate BlockCompressWriter
        SequenceFile.BlockCompressWriter writer =
            new SequenceFile.BlockCompressWriter(conf, options);

        // 4. Read compressionBlockSize via reflection
        Field field = SequenceFile.BlockCompressWriter.class.getDeclaredField("compressionBlockSize");
        field.setAccessible(true);
        int actualBlockSize = (Integer) field.get(writer);

        // 5. Assert the custom value is used
        assertEquals(50000, actualBlockSize);

        // 6. Clean up
        writer.close();
        fs.delete(tempPath, false);
    }
}