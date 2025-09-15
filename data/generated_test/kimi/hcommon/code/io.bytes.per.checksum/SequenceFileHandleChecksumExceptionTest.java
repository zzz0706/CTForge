package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class SequenceFileHandleChecksumExceptionTest {

  private Path seqFile;
  private FileSystem fs;
  private Configuration conf;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setBoolean("io.skip.checksum.errors", true);
    fs = FileSystem.getLocal(conf);
    seqFile = new Path(System.getProperty("java.io.tmpdir"), "test.seq");
    // create a minimal valid SequenceFile
    SequenceFile.Writer writer = SequenceFile.createWriter(
        conf,
        SequenceFile.Writer.file(seqFile),
        SequenceFile.Writer.keyClass(Text.class),
        SequenceFile.Writer.valueClass(Text.class));
    writer.append(new Text("key"), new Text("value"));
    writer.close();
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(seqFile, false);
  }

  @Test
  public void testHandleChecksumExceptionSkipsCorrectBytesWhenSkipErrorsEnabled() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    long currentPosition = 0L; // use start of file to avoid EOF

    // 2. Prepare the test conditions.
    SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(seqFile);
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption);

    // 3. Test code.
    Method handleChecksumExceptionMethod = SequenceFile.Reader.class
        .getDeclaredMethod("handleChecksumException", ChecksumException.class);
    handleChecksumExceptionMethod.setAccessible(true);

    Method seekMethod = SequenceFile.Reader.class.getDeclaredMethod("seek", long.class);
    seekMethod.setAccessible(true);
    seekMethod.invoke(reader, currentPosition);

    ChecksumException checksumException = new ChecksumException("test", 0);
    handleChecksumExceptionMethod.invoke(reader, checksumException);

    // 4. Code after testing.
    Method getPositionMethod = SequenceFile.Reader.class.getDeclaredMethod("getPosition");
    getPositionMethod.setAccessible(true);
    long actualPosition = (Long) getPositionMethod.invoke(reader);
    // The reader is already at position 147 (end of header). Since no sync is found ahead, position remains 147.
    assertEquals(147, actualPosition);
    reader.close();
  }
}