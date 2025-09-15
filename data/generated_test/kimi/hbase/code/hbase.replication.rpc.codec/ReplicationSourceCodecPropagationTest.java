package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.WALFileLengthProvider;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class ReplicationSourceCodecPropagationTest {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(ReplicationSourceCodecPropagationTest.class);

    @Test
    public void customCodecIsPropagatedToRpcCodecKey() throws IOException {
        // 1. Create configuration without explicit set so defaults / test-resource overrides apply
        Configuration conf = HBaseConfiguration.create();

        // 2. Dynamic expected value: read from the same key the code will read
        String expectedCodec = conf.get(HConstants.REPLICATION_CODEC_CONF_KEY,
                                        "org.apache.hadoop.hbase.codec.KeyValueCodecWithTags");

        // 3. Stub external dependencies
        FileSystem mockFs = Mockito.mock(FileSystem.class);
        ReplicationSourceManager mockManager = Mockito.mock(ReplicationSourceManager.class);
        ReplicationQueueStorage mockQueueStorage = Mockito.mock(ReplicationQueueStorage.class);
        ReplicationPeer mockPeer = Mockito.mock(ReplicationPeer.class);
        Server mockServer = Mockito.mock(Server.class);
        WALFileLengthProvider mockWALProvider = Mockito.mock(WALFileLengthProvider.class);

        // 4. Instantiate and invoke the method under test
        ReplicationSource source = new ReplicationSource();
        source.init(conf, mockFs, mockManager, mockQueueStorage, mockPeer, mockServer,
                    "q1", UUID.randomUUID(), mockWALProvider, null);

        // 5. Assertion: the internal configuration should now contain the codec under RPC_CODEC_CONF_KEY
        String actualCodec = source.conf.get(HConstants.RPC_CODEC_CONF_KEY);
        assertEquals(expectedCodec, actualCodec);
    }
}