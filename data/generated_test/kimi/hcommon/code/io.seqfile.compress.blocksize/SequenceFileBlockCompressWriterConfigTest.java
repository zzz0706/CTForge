package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class SequenceFileBlockCompressWriterConfigTest {

    private Configuration conf;
    private Path testPath;
    private FileSystem fs;

    @Before
    public void setUp() throws IOException {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        conf = new Configuration();
        // 2. Prepare the test conditions.
        conf.setInt(CommonConfigurationKeysPublic.IO_SEQFILE_COMPRESS_BLOCKSIZE_KEY, 1000);
        testPath = new Path("target/test/data/test.seq");
        fs = FileSystem.getLocal(conf);
        fs.delete(testPath, true);
    }

    @Test
    public void testNoSyncWhenBlockSizeNotReached() throws IOException {
        // 3. Test code.
        SequenceFile.Writer.Option[] options = {
                SequenceFile.Writer.file(testPath),
                SequenceFile.Writer.keyClass(BytesWritable.class),
                SequenceFile.Writer.valueClass(BytesWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
        };
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, options);
        assertNotNull(writer);

        // Write key-value pair with total size less than block size
        BytesWritable key = new BytesWritable(new byte[499]);
        BytesWritable value = new BytesWritable(new byte[500]);
        writer.append(key, value);
        writer.close();
    }

    @After
    public void tearDown() throws IOException {
        // 4. Code after testing.
        if (fs != null && testPath != null) {
            fs.delete(testPath, true);
        }
    }
}