package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.ClusterStatusPublisher;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({MasterTests.class, SmallTests.class})
public class TestStatusPublisherClassConfiguration {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestStatusPublisherClassConfiguration.class);

    @Test
    public void testStatusPublisherClassConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        String key = ClusterStatusPublisher.STATUS_PUBLISHER_CLASS;
        String defaultName = ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS.getName();
        String clsName = conf.get(key, defaultName);

        assertNotNull(clsName);
        assertFalse(clsName.isEmpty());

        try {
            Class<?> clazz = Class.forName(clsName);
            assertTrue(ClusterStatusPublisher.Publisher.class.isAssignableFrom(clazz));
            ClusterStatusPublisher.Publisher instance =
                (ClusterStatusPublisher.Publisher) ReflectionUtils.newInstance(clazz, conf);
            assertNotNull(instance);
        } catch (ClassNotFoundException e) {
            fail("Publisher class not found: " + clsName);
        }

        assertEquals(defaultName, clsName);
    }
}
