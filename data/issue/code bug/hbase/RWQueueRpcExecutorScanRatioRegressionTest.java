package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Constructor;

import static org.junit.Assert.assertFalse;

//HBASE-16815
@Category({RegionServerTests.class, SmallTests.class})
public class RWQueueRpcExecutorScanRatioRegressionTest {
  
  @Test
  public void testScanRatioZeroDoesNotThrow() throws Exception {
    // 1. Load the user’s HBase configuration
    Configuration conf = new Configuration();
    float scanShare = conf.getFloat("hbase.ipc.server.callqueue.scan.ratio", 0.0f);

    // 2. Only execute on the configuration that sets scan.ratio == 0
    Assume.assumeTrue("User config does not set scan.ratio=0 – test not applicable",
        scanShare == 0.0f);

    // Generic parameters (taken from config or sensible defaults)
    int handlerCount  = conf.getInt("hbase.regionserver.handler.count", 30);
    int numQueues     = conf.getInt("hbase.ipc.server.callqueue.num.queues", 3);
    float readShare   = conf.getFloat("hbase.ipc.server.callqueue.read.ratio", 0.5f);
    int maxQueueLen   = conf.getInt("hbase.ipc.server.max.callqueue.length", 100);

    boolean failed = false;
    Exception thrown = null;

    try {
      Object exec = null;

      // 3-A  try signature: (String,int,int,float,float,int,Configuration,Abortable)
      try {
        Constructor<RWQueueRpcExecutor> c =
            RWQueueRpcExecutor.class.getConstructor(String.class, int.class, int.class,
                float.class, float.class, int.class, Configuration.class, Abortable.class);
        exec = c.newInstance("test", handlerCount, numQueues,
                             readShare, scanShare, maxQueueLen, conf, null);
      } catch (NoSuchMethodException ignore) { /* try next */ }

      // 3-B  try signature: (String,int,int,float,int,Configuration,Abortable)
      if (exec == null) {
        try {
          Constructor<RWQueueRpcExecutor> c =
              RWQueueRpcExecutor.class.getConstructor(String.class, int.class, int.class,
                  float.class, int.class, Configuration.class, Abortable.class);
          exec = c.newInstance("test", handlerCount, numQueues,
                               readShare, maxQueueLen, conf, null);
        } catch (NoSuchMethodException ignore) { /* try next */ }
      }

      // 3-C  try signature: (String,int,int,float,float,int)
      if (exec == null) {
        try {
          Constructor<RWQueueRpcExecutor> c =
              RWQueueRpcExecutor.class.getConstructor(String.class, int.class, int.class,
                  float.class, float.class, int.class);
          exec = c.newInstance("test", handlerCount, numQueues,
                               readShare, scanShare, maxQueueLen);
        } catch (NoSuchMethodException ignore) { /* no suitable ctor */ }
      }

      // If no ctor matched this version, skip the test gracefully
      Assume.assumeTrue("RWQueueRpcExecutor ctor not found on this branch – skipping",
          exec != null);

    } catch (Exception e) {
      failed = true;
      thrown = e;
    }

    if (failed) {
      System.err.println("RWQueueRpcExecutor threw exception under user config: " + thrown);
    }
    assertFalse("RWQueueRpcExecutor should not throw when scan.ratio=0", failed);
  }
}
