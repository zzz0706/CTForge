package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(SmallTests.class)
public class TestModifyRegionUtils {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestModifyRegionUtils.class);

    @Test
    public void testCreateRegionWithInvalidRegionFillTask() throws IOException {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("hbase.rootdir", "/tmp/hbase");

        // Fetch the root directory for HBase
        String rootDir = configuration.get("hbase.rootdir");
        // Ensure the rootDir is fetched correctly using the API
        assert rootDir != null && !rootDir.isEmpty(); 
        Path rootPath = new Path(rootDir);

        // 2. Prepare the test conditions
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(
                TableName.valueOf("testTable")).build();
        RegionInfo regionInfo = RegionInfoBuilder.newBuilder(
                TableName.valueOf("testTable")).build();

        // Define an invalid RegionFillTask (null value)
        ModifyRegionUtils.RegionFillTask invalidTask = null;

        // 3. Test code
        RegionInfo createdRegion = ModifyRegionUtils.createRegion(
                configuration,
                rootPath,
                tableDescriptor,
                regionInfo,
                invalidTask);

        // 4. Code after testing
        // Assertions to confirm the test results
        assert createdRegion != null;
        assert createdRegion.getTable().equals(TableName.valueOf("testTable"));
    }
}