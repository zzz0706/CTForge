package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class SequenceFileChecksumTest {

  @Test
  public void SequenceFile_handleChecksumException_rethrowsWhenSkipErrorsDisabled() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");

    // 2. Prepare the test conditions.
    Path tmp = new Path("file:///tmp/test.seq");
    FileSystem fs = tmp.getFileSystem(conf);
    SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[] {
        SequenceFile.Writer.file(tmp),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(LongWritable.class)
    };
    // create an empty file so the reader can be opened
    try (SequenceFile.Writer writer = SequenceFile.createWriter(conf, opts)) {
      writer.append(new LongWritable(1L), new LongWritable(1L));
    }

    try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(tmp))) {

      ChecksumException expectedException = new ChecksumException("test", 0);

      // 3. Test code.
      IOException thrown = null;
      try {
        // use reflection to access private method
        Method m = SequenceFile.Reader.class.getDeclaredMethod("handleChecksumException", ChecksumException.class);
        m.setAccessible(true);
        m.invoke(reader, expectedException);
      } catch (Exception e) {
        if (e.getCause() instanceof IOException) {
          thrown = (IOException) e.getCause();
        }
      }

      // 4. Code after testing.
      assertEquals("ChecksumException should be re-thrown", expectedException, thrown);
    } finally {
      fs.delete(tmp, false);
    }
  }
}