package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.junit.Test;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit testing for Groups class to ensure configuration usage
 * and static mapping behavior.
 */
public class TestGroupsStaticMapping {

    /**
     * Tests the Groups constructor with an empty static mapping.
     */
    @Test
    public void testGroupsConstructorWithEmptyStaticMapping() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Set the hadoop.user.group.static.mapping.overrides property to an empty value.
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "");

        // Step 3: Instantiate Groups with the configuration and a mock timer.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 4: Verify that staticMapRef is null when no static mappings are provided.
        assertNull("Expected staticMapRef to be null for empty static mappings.", groups.getStaticMap());
    }

    /**
     * Tests the Groups constructor with valid static mappings.
     */
    @Test
    public void testGroupsConstructorWithValidStaticMapping() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Set the hadoop.user.group.static.mapping.overrides property with valid mappings.
        String validMapping = "user1=group1,group2;user2=group3";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);

        // Step 3: Instantiate Groups with the configuration and a mock timer.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 4: Verify that staticMapRef is populated with the expected mappings.
        Map<String, List<String>> staticMapping = groups.getStaticMap();
        assertNotNull("staticMapRef should not be null for valid mappings.", staticMapping);
        assertTrue("Mapping for user1 should include group1 and group2.", staticMapping.containsKey("user1"));
        assertTrue("Mapping for user2 should include group3.", staticMapping.containsKey("user2"));
    }

    /**
     * Tests the Groups constructor with malformed static mappings.
     */
    @Test
    public void testGroupsConstructorWithMalformedStaticMapping() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Set the hadoop.user.group.static.mapping.overrides property to an invalid value.
        String malformedMapping = "user1group1,user2="; // Missing '=' delimiter.
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, malformedMapping);

        // Step 3: Verify that creating a Groups instance throws HadoopIllegalArgumentException.
        Timer timer = new Timer();
        assertThrows("Expected HadoopIllegalArgumentException for malformed mappings.",
                HadoopIllegalArgumentException.class, () -> new Groups(conf, timer));
    }

    /**
     * Tests that parseStaticMapping is indirectly covered via Groups constructor.
     */
    @Test
    public void testParseStaticMappingThroughConstructor() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Set the hadoop.user.group.static.mapping.overrides property with valid mappings.
        String validMapping = "userA=team1,team2;userB=team3";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);

        // Step 3: Instantiate Groups and validate internal mapping.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 4: Validate that the mappings were correctly parsed.
        Map<String, List<String>> staticMapping = groups.getStaticMap();
        assertNotNull("staticMapRef should contain parsed mappings.", staticMapping);
        assertEquals("team1,team2", String.join(",", staticMapping.get("userA")));
    }
}