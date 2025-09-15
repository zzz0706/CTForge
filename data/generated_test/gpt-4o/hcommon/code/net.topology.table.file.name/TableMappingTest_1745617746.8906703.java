package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.TableMapping;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableMappingTest {       
    // test code
    // 1. 使用API获取配置值，不要硬编码配置值
    // 2. 准备测试条件
    // 3. 测试代码
    // 4. 测试后的代码
  
    @Test
    public void testLoadWithBlankConfiguration() {
        // Step 1: Create a Configuration object without setting "net.topology.table.file.name"
        Configuration conf = new Configuration();
        
        // Step 2: Initialize the TableMapping instance with the configuration
        TableMapping tableMapping = new TableMapping();
        tableMapping.setConf(conf);
        
        // Step 3: Create the input list for resolution
        List<String> inputNames = new ArrayList<>();
        inputNames.add("host1.example.com");
        
        // Step 4: Call the resolve method
        List<String> resolvedNames = tableMapping.resolve(inputNames);
        
        // Step 5: Verify that the default rack is applied for all nodes due to blank configuration
        assertEquals(1, resolvedNames.size());
        assertEquals("/default-rack", resolvedNames.get(0));
    }
}