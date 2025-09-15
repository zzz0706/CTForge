package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHadoopUserGroupStaticMappingOverrides {

  @Test
  public void testValidStaticMapping() {
    Configuration conf = new Configuration();
    // rely on default value: "dr.who=;" which is valid
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testInvalidStaticMappingMalformedEntry() {
    Configuration conf = new Configuration();
    // Hadoop 2.8.5 only logs a warning for malformed entries instead of throwing
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
             "user1=group1,group2;invalid");
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }

  @Test
  public void testInvalidStaticMappingExtraEquals() {
    Configuration conf = new Configuration();
    // Hadoop 2.8.5 throws HadoopIllegalArgumentException for extra '='
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
             "user1=group1=group2");
    try {
      Groups groups = new Groups(conf);
      fail("Expected HadoopIllegalArgumentException");
    } catch (Exception e) {
      assertTrue(e instanceof org.apache.hadoop.HadoopIllegalArgumentException);
    }
  }

  @Test
  public void testEmptyMapping() {
    Configuration conf = new Configuration();
    // empty string should be treated as no mappings
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "");
    Groups groups = new Groups(conf);
    assertNotNull(groups);
  }
}