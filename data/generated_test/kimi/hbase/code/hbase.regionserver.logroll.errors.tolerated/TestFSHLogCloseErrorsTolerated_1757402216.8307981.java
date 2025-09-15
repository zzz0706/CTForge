package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFSHLogCloseErrorsTolerated {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSHLogCloseErrorsTolerated.class);

  @Test
  public void testDefaultCloseErrorsToleratedIsLoadedFromConfiguration() throws Exception {
    // 1. Configuration as Input
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions
    Path rootDir = new Path("file:///tmp/hbase-test");
    FileSystem fs = rootDir.getFileSystem(conf);

    // 3. Test code
    FSHLog fshLog = new FSHLog(fs, rootDir, "logs", "oldlogs",
        conf, null, false, "test", null);

    // 4. Code after testing
    Field closeErrorsToleratedField = FSHLog.class.getDeclaredField("closeErrorsTolerated");
    closeErrorsToleratedField.setAccessible(true);
    int actualCloseErrorsTolerated = (int) closeErrorsToleratedField.get(fshLog);

    assertEquals(conf.getInt("hbase.regionserver.logroll.errors.tolerated", 2),
                 actualCloseErrorsTolerated);
  }
}