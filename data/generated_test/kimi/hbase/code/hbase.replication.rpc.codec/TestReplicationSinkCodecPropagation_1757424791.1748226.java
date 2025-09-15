package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

@Category(SmallTests.class)
public class TestReplicationSinkCodecPropagation {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestReplicationSinkCodecPropagation.class);

    @Test
    public void defaultCodecIsPropagatedToRpcCodecKey() throws Exception {
        // 1. Create a fresh Configuration without any overrides
        Configuration conf = HBaseConfiguration.create();

        // 2. Instantiate ReplicationSink which internally calls decorateConf()
        Stoppable mockStopper = mock(Stoppable.class);
        ReplicationSink sink = new ReplicationSink(conf, mockStopper);

        // 3. Read the propagated value from the configuration used by ReplicationSink
        //    ReplicationSink copies the passed Configuration internally, so we must
        //    read it back via reflection since getConfiguration() is not exposed.
        Field confField = ReplicationSink.class.getDeclaredField("conf");
        confField.setAccessible(true);
        Configuration actualConf = (Configuration) confField.get(sink);
        String actualCodec = actualConf.get(HConstants.RPC_CODEC_CONF_KEY);

        // 4. Compute the expected default value dynamically
        String expectedCodec = conf.get(HConstants.REPLICATION_CODEC_CONF_KEY,
                "org.apache.hadoop.hbase.codec.KeyValueCodecWithTags");

        // 5. Assert the codec was correctly propagated
        assertEquals(expectedCodec, actualCodec);
    }
}