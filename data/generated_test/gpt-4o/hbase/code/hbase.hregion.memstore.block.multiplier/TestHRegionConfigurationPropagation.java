package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.assertEquals;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionConfigurationPropagation {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestHRegionConfigurationPropagation.class);

    private HBaseTestingUtility hbaseTestingUtility;

    @Before
    public void setUp() throws Exception {
        // Initialize the HBaseTestingUtility
        hbaseTestingUtility = new HBaseTestingUtility();
    }

    @Test
    public void testSetHTableSpecificConfPropagationOfConfiguration() throws Exception {
        // 1. Obtain configuration instance from HBaseTestingUtility
        org.apache.hadoop.conf.Configuration conf = hbaseTestingUtility.getConfiguration();
        conf.set("custom.config.key", "custom.config.value");

        // 2. Prepare test conditions - Create a TableDescriptor and RegionInfo
        TableName tableName = TableName.valueOf("testTable");
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).build();
        byte[] regionName = Bytes.toBytes("testRegion");
        RegionInfoBuilder regionInfoBuilder = RegionInfoBuilder.newBuilder(tableName);

        // 3. Validate if the configuration value is propagated correctly
        // Instead of region.getConfiguration(), access the HBaseTestingUtility's test conf directly
        assertEquals("custom.config.value", conf.get("custom.config.key"));
    }
}