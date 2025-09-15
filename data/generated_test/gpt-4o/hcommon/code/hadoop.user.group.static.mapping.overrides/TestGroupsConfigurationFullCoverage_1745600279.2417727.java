package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestGroupsConfigurationFullCoverage {

    /**
     * Test to verify the Groups constructor handles an empty static mapping configuration.
     */
    @Test
    public void testGroupsConstructorWithEmptyStaticMapping() {
        // Step 1: Create a configuration object without any static mapping.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "");

        // Step 2: Instantiate the Groups object.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Check that staticMapRef is set to null.
        assertNull("Expected staticMapRef to be null when no static mapping is provided.", groups.getStaticMap());
    }

    /**
     * Test to verify Groups constructor with valid static mapping configuration.
     */
    @Test
    public void testGroupsConstructorWithValidStaticMapping() {
        // Step 1: Set up the configuration object with valid static mapping overrides.
        Configuration conf = new Configuration();
        String validMapping = "user1=group1,group2;user2=group3";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);

        // Step 2: Instantiate the Groups object.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Verify the parsed mappings are correct.
        assertNotNull("Expected static mappings to be present.", groups.getStaticMap());
        assertEquals(List.of("group1", "group2"), groups.getStaticMap().get("user1"));
        assertEquals(List.of("group3"), groups.getStaticMap().get("user2"));
    }

    /**
     * Test to verify Groups constructor with malformed static mapping configuration.
     */
    @Test(expected = HadoopIllegalArgumentException.class)
    public void testGroupsConstructorWithMalformedMapping() {
        // Step 1: Set up configuration object with malformed overrides.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1group1,user2=invalid");

        // Step 2: Instantiate Groups object, expecting an exception.
        Timer timer = new Timer();
        new Groups(conf, timer);
    }

    /**
     * Test default behavior when no static mapping configuration is provided.
     */
    @Test
    public void testGroupsDefaultBehaviorWithoutMapping() {
        // Step 1: Set up configuration object without static mapping overrides.
        Configuration conf = new Configuration();

        // Step 2: Instantiate Groups object.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Validate that staticMapRef is null when no mapping is provided.
        assertNull("Expected static mappings to be null.", groups.getStaticMap());
    }

    /**
     * Test caching mechanism interaction with static mappings.
     */
    @Test
    public void testGroupsCachingWithStaticMapping() {
        // Step 1: Initialize a configuration with valid static mapping overrides.
        Configuration conf = new Configuration();
        String validMapping = "userA=groupX,groupY;userB=groupZ";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);

        // Step 2: Instantiate Groups object.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Validate caching and mappings integration.
        assertNotNull("Cache should be initialized.", groups.getCache());
        assertTrue("Expected userA mappings to be present.", groups.getStaticMap().containsKey("userA"));
        assertTrue("Expected userB mappings to be present.", groups.getStaticMap().containsKey("userB"));
    }
}