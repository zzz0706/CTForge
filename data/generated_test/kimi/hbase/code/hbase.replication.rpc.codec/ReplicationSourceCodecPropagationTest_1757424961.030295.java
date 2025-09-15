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
        // 1. Create configuration and explicitly set the custom codec
        Configuration conf = HBaseConfiguration.create();
        String customCodec = "com.example.CustomCodec";
        conf.set(HConstants.REPLICATION_CODEC_CONF_KEY, customCodec);

        // 2. Prepare stubs for external dependencies
        FileSystem mockFs = Mockito.mock(FileSystem.class);
        ReplicationSourceManager mockManager = Mockito.mock(ReplicationSourceManager.class);
        ReplicationQueueStorage mockQueueStorage = Mockito.mock(ReplicationQueueStorage.class);
        ReplicationPeer mockPeer = Mockito.mock(ReplicationPeer.class);
        Server mockServer = Mockito.mock(Server.class);
        WALFileLengthProvider mockWALProvider = Mockito.mock(WALFileLengthProvider.class);

        // 3. Instantiate ReplicationSource and invoke init()
        ReplicationSource source = new ReplicationSource();
        source.init(conf, mockFs, mockManager, mockQueueStorage, mockPeer, mockServer,
                    "q1", UUID.randomUUID(), mockWALProvider, null);

        // 4. Verify that the custom codec is propagated to RPC_CODEC_CONF_KEY
        String actualCodec = source.conf.get(HConstants.RPC_CODEC_CONF_KEY);
        assertEquals(customCodec, actualCodec);
    }
}