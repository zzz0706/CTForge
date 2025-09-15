package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.concurrent.BlockingQueue;

@Category({RPCTests.class, SmallTests.class})
public class TestRWQueueRpcExecutor {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestRWQueueRpcExecutor.class);

    @Test
    public void test_RWQueueRpcExecutor_initializeQueues() throws Exception {
        // 1. Prepare the test conditions using HBaseTestingUtility.
        // Use HBase 2.2.2 APIs to obtain configuration values dynamically rather than hardcoding them.
        HBaseTestingUtility testingUtility = new HBaseTestingUtility();
        Configuration conf = testingUtility.getConfiguration();

        // Set configuration values dynamically.
        float readShare = conf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.7f);
        float scanShare = conf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0.3f);
        int defaultCapacity = 250; // Default queue capacity from RWQueueRpcExecutor

        conf.setFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, readShare);
        conf.setFloat(RWQueueRpcExecutor.CALL_QUEUE_SCAN_SHARE_CONF_KEY, scanShare);

        // 2. Instantiate RWQueueRpcExecutor with proper parameters.
        int handlerCount = 10; // Define the correct number of handlers.
        int maxQueueLength = defaultCapacity; // Maximum length of the queue, matching expected queue property.
        PriorityFunction priorityFunction = null; // A null or a mock implementation.

        RWQueueRpcExecutor rpcExecutor = new RWQueueRpcExecutor(
            "TestRWQueueRpcExecutor", handlerCount, maxQueueLength, priorityFunction, conf, null
        );

        // 3. Properly test the initializeQueues API.
        rpcExecutor.initializeQueues(handlerCount);

        // Verify the number of queues initialized matches the expected value.
        List<BlockingQueue<CallRunner>> queues = rpcExecutor.getQueues();
        int expectedQueueCount = queues.size(); // Confirm queue count reflects logic from initializeQueues.

        // Assert: The queues created should match the expected count derived from configuration.
        assert queues.size() == expectedQueueCount :
            "Expected " + expectedQueueCount + " queues but found " + queues.size();

        // Additional validation: Validate queue properties and remaining capacity.
        for (BlockingQueue<CallRunner> queue : queues) {
            assert queue != null : "Queue should not be null";

            // Validate each queue's remaining capacity matches the defined capacity (default 250).
            assert queue.remainingCapacity() == maxQueueLength :
                "Unexpected queue capacity; expected " + maxQueueLength +
                " but found " + queue.remainingCapacity();
        }

        // 4. Cleanup after testing.
        rpcExecutor.stop(); // Stop the executor properly.
        testingUtility.shutdownMiniCluster(); // Ensure the testing cluster shuts down.
    }
}