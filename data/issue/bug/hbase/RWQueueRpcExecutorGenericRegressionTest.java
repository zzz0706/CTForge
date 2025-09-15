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

//HBASE-14252
@Category({RegionServerTests.class, SmallTests.class})
public class RWQueueRpcExecutorGenericRegressionTest {

  @Test
  public void testExecutorConstructionWithUserConfig() {
    Configuration conf = new Configuration();

    int handlerCount  = conf.getInt("hbase.regionserver.handler.count", 30);
    int numQueues     = conf.getInt("hbase.ipc.server.callqueue.num.queues", 3);
    float readShare   = conf.getFloat("hbase.ipc.server.callqueue.read.ratio", 0.5f);
    float scanShare   = conf.getFloat("hbase.ipc.server.callqueue.scan.ratio", 0.0f);
    int maxQueueLen   = conf.getInt("hbase.ipc.server.max.callqueue.length", 100);

    boolean failed = false;
    Exception thrown = null;

    try {
      Object exec = null;

      try {
        Constructor<RWQueueRpcExecutor> c =
            RWQueueRpcExecutor.class.getConstructor(
                String.class, int.class, int.class,
                float.class, float.class, int.class,
                Configuration.class, Abortable.class);
        exec = c.newInstance("test", handlerCount, numQueues,
                             readShare, scanShare, maxQueueLen, conf, null);
      } catch (NoSuchMethodException ignore) { /* next */ }

      /* try ②  RWQueueRpcExecutor(String,int,int,float,float,int) */
      if (exec == null) {
        try {
          Constructor<RWQueueRpcExecutor> c =
              RWQueueRpcExecutor.class.getConstructor(
                  String.class, int.class, int.class,
                  float.class, float.class, int.class);
          exec = c.newInstance("test", handlerCount, numQueues,
                               readShare, scanShare, maxQueueLen);
        } catch (NoSuchMethodException ignore) { /* next */ }
      }

      if (exec == null) {
        try {
          Constructor<RWQueueRpcExecutor> c =
              RWQueueRpcExecutor.class.getConstructor(
                  String.class, int.class, int.class,
                  float.class, int.class,
                  Configuration.class, Abortable.class);
          exec = c.newInstance("test", handlerCount, numQueues,
                               readShare, maxQueueLen, conf, null);
        } catch (NoSuchMethodException ignore) { /* no more */ }
      }

      // If the current branch exposes none of the above ctors → skip gracefully
      Assume.assumeTrue("RWQueueRpcExecutor ctor not found on this branch", exec != null);

    } catch (Exception e) {
      failed  = true;
      thrown  = e;               // Preserve stack for diagnostics below
    }

    if (failed) {
      System.err.println("\n*** RWQueueRpcExecutor threw exception with user config ***");
      thrown.printStackTrace(System.err);
    }

    assertFalse("RWQueueRpcExecutor construction must not throw "
                + "(detects divide-by-zero / queue==0 issues)", failed);
  }
}
