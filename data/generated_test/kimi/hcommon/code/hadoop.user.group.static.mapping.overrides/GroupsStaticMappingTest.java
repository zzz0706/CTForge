package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GroupsStaticMappingTest {

  @Test
  public void verifyCustomSingleUserSingleGroupMapping() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "alice=staff");
    Groups groups = new Groups(conf);

    // 3. Test code.
    List<String> groupsForAlice = groups.getGroups("alice");
    assertNotNull(groupsForAlice);
    assertEquals(1, groupsForAlice.size());
    assertEquals("staff", groupsForAlice.get(0));

    // 4. Code after testing.
  }

}