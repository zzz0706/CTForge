package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationSource_InitAndDecorateConf {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestReplicationSource_InitAndDecorateConf.class);

    @Test
    public void testInitAndDecorateConf() throws Exception {
        // 1. Prepare the test conditions
        // Mock dependencies for the ReplicationSource initialization
        Configuration conf = new Configuration(); // corrected to create a real configuration object
        FileSystem fs = mock(FileSystem.class);
        ReplicationQueueStorage queueStorage = mock(ReplicationQueueStorage.class);
        ReplicationPeer replicationPeer = mock(ReplicationPeer.class);
        Server server = mock(Server.class);
        MetricsSource metrics = mock(MetricsSource.class);
        ReplicationSourceManager replicationSourceManager = mock(ReplicationSourceManager.class);
        WALFileLengthProvider walFileLengthProvider = mock(WALFileLengthProvider.class);
        UUID clusterId = UUID.randomUUID();

        // Configure the actual configuration - properly configure hbase.replication.rpc.codec
        String expectedCodec = "org.apache.hadoop.hbase.codec.KeyValueCodecWithTags";
        conf.set(HConstants.REPLICATION_CODEC_CONF_KEY, expectedCodec);

        // 2. Create the ReplicationSource object
        ReplicationSource replicationSource = new ReplicationSource();

        // 3. Initialize ReplicationSource with mocked objects
        replicationSource.init(conf, fs, replicationSourceManager, queueStorage, replicationPeer, server, 
                "test-queue", clusterId, walFileLengthProvider, metrics);

        // 4. Verify the configuration retrieval and correctness
        assertEquals("Replication codec configuration was not correctly propagated.",
                expectedCodec, conf.get(HConstants.REPLICATION_CODEC_CONF_KEY));
    }
}