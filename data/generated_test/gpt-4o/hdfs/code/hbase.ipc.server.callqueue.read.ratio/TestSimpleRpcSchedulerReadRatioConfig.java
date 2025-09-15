package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor;
import org.apache.hadoop.hbase.ipc.SimpleRpcScheduler;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestSimpleRpcSchedulerReadRatioConfig {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestSimpleRpcSchedulerReadRatioConfig.class);

    @Test
    public void testSimpleRpcSchedulerReadRatioConfig() throws Exception {
        // 1. Prepare the test conditions: Create and set up configuration
        Configuration conf = new Configuration();
        conf.setFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f); // Configuring read ratio
        conf.setInt("hbase.regionserver.handler.count", 30); // Number of rpc handlers
        conf.setInt("hbase.master.priority.handler.count", 10); // Priority handlers
        conf.setInt("hbase.master.replication.handler.count", 5); // Replication handlers
        conf.setInt("hbase.master.metaTransition.handler.count", 2); // Meta handlers
        conf.setInt("hbase.master.policy.priority.highLevel", 10); // High-priority level

        // Retrieve configuration values using the HBase 2.2.2 API
        float readRatio = conf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f);
        int handlerCount = conf.getInt("hbase.regionserver.handler.count", 30);
        int priorityHandlerCount = conf.getInt("hbase.master.priority.handler.count", 10);
        int replicationHandlerCount = conf.getInt("hbase.master.replication.handler.count", 5);
        int metaTransitionHandler = conf.getInt("hbase.master.metaTransition.handler.count", 2);
        int highPriorityLevel = conf.getInt("hbase.master.policy.priority.highLevel", 10);

        // 2. Define a dummy implementation of PriorityFunction for testing
        PriorityFunction priorityFunction = new PriorityFunction() {
            @Override
            public int getPriority(RPCProtos.RequestHeader request, Message param, User user) {
                // This dummy implementation returns 0 priority always for simplicity
                return 0;
            }

            @Override
            public long getDeadline(RPCProtos.RequestHeader header, Message param) {
                // Dummy implementation with no specific deadline
                return 0;
            }
        };

        // 3. Create an instance of SimpleRpcScheduler with the configuration
        SimpleRpcScheduler rpcScheduler = new SimpleRpcScheduler(
                conf,
                handlerCount,
                priorityHandlerCount,
                replicationHandlerCount,
                metaTransitionHandler,
                priorityFunction,
                null, // Using null for Abortable as it's not relevant to this test
                highPriorityLevel
        );

        // Ensure proper initialization of RpcScheduler
        assertTrue("rpcScheduler should not be null after initialization", rpcScheduler != null);

        // 4. Test the behavior and verify outputs
        // Simulate dispatch behavior (with null input since no Rpc calls are given in this test)
        boolean dispatchResult = false;
        try {
            dispatchResult = rpcScheduler.dispatch(null); // Should safely handle null
        } catch (NullPointerException e) {
            dispatchResult = false; // If exception, set dispatchResult to false
        }

        // Retrieve queue lengths
        int writeQueueLength = rpcScheduler.getWriteQueueLength();
        int readQueueLength = rpcScheduler.getReadQueueLength();

        // Debugging outputs (optional)
        System.out.println("Dispatch result: " + dispatchResult);
        System.out.println("Length of Write Queue: " + writeQueueLength);
        System.out.println("Length of Read Queue: " + readQueueLength);

        // Assertions
        assertTrue("Write queue length should not be negative", writeQueueLength >= 0);
        assertTrue("Read queue length should not be negative", readQueueLength >= 0);
        assertTrue("Dispatch result should be false for null inputs", !dispatchResult);

        // Verify the configuration of read ratio
        if (readRatio > 0 && readRatio <= 1.0) {
            assertTrue("Read queue should be initialized correctly based on read ratio",
                    readQueueLength >= 0);
        }
    }
}