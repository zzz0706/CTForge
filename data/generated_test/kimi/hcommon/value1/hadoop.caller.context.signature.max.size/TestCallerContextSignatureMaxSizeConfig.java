package org.apache.hadoop.conf;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestCallerContextSignatureMaxSizeConfig {

  @Test
  public void testCallerContextSignatureMaxSizeIsValidInt() {
    Configuration conf = new Configuration();
    // 1. Obtain the value using the public key
    int maxSize = conf.getInt(
        CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY,
        40);

    // 2 & 3. Validate that the value is a non-negative integer
    assertTrue("hadoop.caller.context.signature.max.size must be >= 0",
               maxSize >= 0);
  }
}