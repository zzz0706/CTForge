package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({MediumTests.class, ReplicationTests.class})
public class TestReplicationSink_DecorateConfForClientRetries {

    @ClassRule // HBaseClassTestRule is applied to this class for test categorization and configuration.
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestReplicationSink_DecorateConfForClientRetries.class);

    @Test
    public void testReplicationSinkDecorateConfForClientRetries() throws Exception {
        // 1. Use the HBase 2.2.2 API correctly.
        // 2. Prepare the test conditions.
        Configuration mockConfiguration = HBaseConfiguration.create();
        mockConfiguration.setInt("replication.sink.client.retries.number", 4);
        mockConfiguration.setInt("replication.sink.client.ops.timeout", 10000);

        // Use an explicit implementation of the Stoppable interface as it requires multiple methods.
        Stoppable mockStoppable = new Stoppable() {
            private volatile boolean stopped = false;

            @Override
            public void stop(String why) {
                this.stopped = true;
            }

            @Override
            public boolean isStopped() {
                return stopped;
            }
        };

        // 3. Test code.
        ReplicationSink replicationSink = new ReplicationSink(mockConfiguration, mockStoppable);

        // Assert that ReplicationSink is initialized correctly.
        assertNotNull(replicationSink);

        // Validate configuration values. 
        // This utilizes the Configuration object directly since ReplicationSink doesn't have a getConf() method.
        int actualRetriesNumber = mockConfiguration.getInt("replication.sink.client.retries.number", -1);
        int actualOpsTimeout = mockConfiguration.getInt("replication.sink.client.ops.timeout", -1);

        // Assert and verify the expected configuration propagation.
        assertEquals(4, actualRetriesNumber);
        assertEquals(10000, actualOpsTimeout);

        // 4. Code after testing.
        mockStoppable.stop("Test complete");
        assertEquals(true, mockStoppable.isStopped());
    }
}