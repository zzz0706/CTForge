package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BloomMapFileConfigTest {

  @Test
  public void verifyCustomErrorRateOverridesDefaultAndAdjustsVectorSize() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    conf.setFloat("io.mapfile.bloom.error.rate", 0.001f);

    // 2. Prepare the test conditions.
    Path dir = new Path("target/test/data/BloomMapFileConfigTest");
    FileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }

    // 3. Test code.
    BloomMapFile.Writer writer = new BloomMapFile.Writer(conf, fs, dir.toString(),
        Text.class, Text.class);
    writer.close();

    // 4. Code after testing.
    assertTrue(fs.exists(dir));
    fs.delete(dir, true);
  }
}