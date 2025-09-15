package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RPCTests.class, SmallTests.class })
public class TestSimpleRpcSchedulerReadRatioZero {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSimpleRpcSchedulerReadRatioZero.class);

  @Test
  public void testReadRatioZeroCreatesBalancedQueueExecutor() throws Exception {
    // 1. Create a fresh Configuration and set required values via the HBase 2.2.2 API
    Configuration conf = new Configuration();
    // Explicitly set the read ratio to 0
    conf.setFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0f);
    // Use a non-FIFO queue type
    conf.set(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY, "deadline");

    // 2. Prepare other required constructor arguments
    int handlerCount = 10;
    int priorityHandlerCount = 0;
    int replicationHandlerCount = 0;
    int metaTransitionHandler = 0;
    PriorityFunction priority = mock(PriorityFunction.class);
    Abortable server = mock(Abortable.class);
    int highPriorityLevel = 0;

    // 3. Instantiate SimpleRpcScheduler under test
    SimpleRpcScheduler scheduler = new SimpleRpcScheduler(
        conf,
        handlerCount,
        priorityHandlerCount,
        replicationHandlerCount,
        metaTransitionHandler,
        priority,
        server,
        highPriorityLevel);

    // 4. Verify the executor type via reflection
    java.lang.reflect.Field callExecutorField = SimpleRpcScheduler.class
        .getDeclaredField("callExecutor");
    callExecutorField.setAccessible(true);
    Object callExecutor = callExecutorField.get(scheduler);

    assertTrue("Expected BalancedQueueRpcExecutor when read ratio is 0",
        callExecutor instanceof BalancedQueueRpcExecutor);
  }
}