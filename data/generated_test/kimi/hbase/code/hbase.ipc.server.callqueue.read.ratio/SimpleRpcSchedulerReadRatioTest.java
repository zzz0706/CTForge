package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RPCTests.class, SmallTests.class})
public class SimpleRpcSchedulerReadRatioTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(SimpleRpcSchedulerReadRatioTest.class);

  @Test
  public void testNegativeReadRatioClampedToZero() throws Exception {
    // 1. Instantiate Configuration
    Configuration conf = new Configuration();

    // 2. Stub the configuration to return a negative value
    Configuration spyConf = org.mockito.Mockito.spy(conf);
    when(spyConf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0f))
        .thenReturn(-0.1f);

    // 3. Prepare other required arguments for SimpleRpcScheduler
    int handlerCount = 10;
    int priorityHandlerCount = 0;
    int replicationHandlerCount = 0;
    int metaTransitionHandler = 0;
    PriorityFunction priority = mock(PriorityFunction.class);
    Abortable server = mock(Abortable.class);
    int highPriorityLevel = 0;

    // 4. Instantiate SimpleRpcScheduler
    SimpleRpcScheduler scheduler = new SimpleRpcScheduler(
        spyConf,
        handlerCount,
        priorityHandlerCount,
        replicationHandlerCount,
        metaTransitionHandler,
        priority,
        server,
        highPriorityLevel);

    // 5. Assert that RWQueueRpcExecutor was bypassed and BalancedQueueRpcExecutor is used
    Field callExecutorField = SimpleRpcScheduler.class.getDeclaredField("callExecutor");
    callExecutorField.setAccessible(true);
    Object callExecutor = callExecutorField.get(scheduler);
    assertTrue("Expected BalancedQueueRpcExecutor when read ratio <= 0",
        callExecutor instanceof BalancedQueueRpcExecutor);
  }
}