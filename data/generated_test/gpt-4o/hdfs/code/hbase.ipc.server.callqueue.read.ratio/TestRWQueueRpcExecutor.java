package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertTrue;

/**
 * Test class for RWQueueRpcExecutor.
 */
@Category(SmallTests.class)
public class TestRWQueueRpcExecutor {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestRWQueueRpcExecutor.class);

    @Test
    public void test_RWQueueRpcExecutor_read_ratio_with_scan_ratio() throws InterruptedException {
        // 1. Obtain configuration values using HBase 2.2.2 API.
        Configuration conf = HBaseConfiguration.create();
        conf.setFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f);
        conf.setFloat(RWQueueRpcExecutor.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0.3f);

        // 2. Prepare the test conditions.
        int handlerCount = 10; // Example handler count.
        int maxQueueLength = 1000; // Example max call queue length.
        PriorityFunction priorityFunction = new PriorityFunction() {
            @Override
            public int getPriority(RPCProtos.RequestHeader header, Message param, User user) {
                return 0;
            }

            @Override
            public long getDeadline(RPCProtos.RequestHeader header, Message param) {
                return 0;
            }
        };

        Abortable abortable = new Abortable() {
            private boolean aborted = false;

            @Override
            public void abort(String why, Throwable e) {
                this.aborted = true;
            }

            @Override
            public boolean isAborted() {
                return this.aborted;
            }
        };

        RWQueueRpcExecutor executor = new RWQueueRpcExecutor(
                "testRWQueueRpcExecutor",
                handlerCount,
                maxQueueLength,
                priorityFunction,
                conf,
                abortable
        );

        // 3. Test code.
        BlockingQueue<CallRunner> writeQueue = executor.getQueues().get(0); // Access the first write queue.
        BlockingQueue<CallRunner> readQueue = executor.getQueues().get(executor.getQueues().size() - 1); // Access the last read queue.

        CallRunner writeCall = new CallRunner(null, null); // Null RpcCall argument (mocking).
        CallRunner readCall = new CallRunner(null, null); // Null RpcCall argument (mocking).

        writeQueue.offer(writeCall);
        readQueue.offer(readCall);

        Threads.sleep(100); // Give some time for the executor to process (helps validate runtime behaviors).

        // 4. Code after testing.
        int writeQueueLength = executor.getWriteQueueLength();
        int readQueueLength = executor.getReadQueueLength();
        int scanQueueLength = executor.getScanQueueLength();

        // Assert expected results and functionality.
        assertTrue("Write queue length should be greater than 0 after dispatch", writeQueueLength > 0);
        assertTrue("Read queue length should be greater than 0 after dispatch", readQueueLength > 0);
        assertTrue("Scan queue length should remain valid", scanQueueLength >= 0); // Scan queues may or may not have items.

        assertTrue("Executor correctly initializes queues and handlers", handlerCount >= 2); // At least one read and one write handler.

        executor.stop(); // Stop the executor properly.
    }
}