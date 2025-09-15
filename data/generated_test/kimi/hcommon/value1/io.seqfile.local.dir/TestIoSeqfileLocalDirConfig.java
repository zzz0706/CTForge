package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.util.StringTokenizer;

import static org.junit.Assert.*;

public class TestIoSeqfileLocalDirConfig {

  @Test
  public void testIoSeqfileLocalDirValid() {
    Configuration conf = new Configuration();
    // 1. Obtain the configuration value via the hadoop-common2.8.5 API
    String dirs = conf.get("io.seqfile.local.dir");

    // 2. Prepare test conditions
    // dirs can be null → fall back to ${hadoop.tmp.dir}/io/local
    // otherwise it must be a comma-separated list of directories
    if (dirs == null || dirs.trim().isEmpty()) {
      // Nothing to validate; default will be used
      return;
    }

    // 3. Test code
    StringTokenizer st = new StringTokenizer(dirs, ",");
    while (st.hasMoreTokens()) {
      String dir = st.nextToken().trim();
      // 4. Validate each path
      assertFalse("Empty path segment in io.seqfile.local.dir", dir.isEmpty());
      // Only check syntactic validity; do not require the directory to exist
      try {
        new Path(dir).toUri();   // triggers URI parsing
      } catch (Exception e) {
        fail("Invalid path in io.seqfile.local.dir: " + dir);
      }
    }
  }
}