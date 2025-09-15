package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class MapFileConfigurationTest {

  @Test
  public void testIndexIntervalFromConfiguration() throws IOException {
    // 1. Create Configuration and set the desired value
    Configuration conf = new Configuration();
    conf.setInt("io.map.index.interval", 77);

    // 2. Calculate the expected value dynamically
    long expectedIndexInterval = conf.getInt("io.map.index.interval", 128);

    // 3. Prepare the test conditions: use local filesystem to avoid real HDFS
    Path tempDir = new Path("file:///tmp/testIndexInterval");
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(tempDir, true); // ensure directory does not exist
    fs.mkdirs(tempDir);

    // 4. Test code: create MapFile.Writer and check index interval
    MapFile.Writer writer = new MapFile.Writer(conf, tempDir,
        MapFile.Writer.keyClass(Text.class),
        MapFile.Writer.valueClass(Text.class));
    long actualIndexInterval = writer.getIndexInterval();
    assertEquals(expectedIndexInterval, actualIndexInterval);

    // 5. Code after testing: close writer and clean up
    writer.close();
    fs.delete(tempDir, true);
  }
}