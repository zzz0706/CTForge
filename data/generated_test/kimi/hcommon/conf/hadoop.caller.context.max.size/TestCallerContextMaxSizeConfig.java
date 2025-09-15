package org.apache.hadoop.conf;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestCallerContextMaxSizeConfig {

  @Test
  public void testCallerContextMaxSizeIsPositiveInt() {
    Configuration conf = new Configuration();
    conf.addResource("core-default.xml");
    conf.addResource("core-site.xml");

    int maxSize = conf.getInt(
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY,
        128);

    assertTrue("hadoop.caller.context.max.size must be a positive integer",
               maxSize > 0);
  }
}