package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import java.util.Collection;

public class TestGroupsStaticMapping {
    // Get configuration value using API
    // Prepare the input conditions for unit testing.

    @Test
    public void testGroupsConstructorWithEmptyStaticMapping() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Set the `hadoop.user.group.static.mapping.overrides` property explicitly to an empty string.
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "");

        // Step 3: Instantiate the `Groups` class using configuration and a mock Timer.
        Timer mockTimer = new Timer();
        Groups groups = new Groups(conf, mockTimer);

        // Step 4: Verify that the internal `staticMapRef` is null, since no static mappings are provided.
        assertNull("The staticMapRef should be null when the static mapping is empty.", groups.getStaticMap());
    }

    @Test
    public void testGroupsConstructorWithValidStaticMapping() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Set the `hadoop.user.group.static.mapping.overrides` property explicitly to valid static mappings.
        String validMapping = "user1=group1,group2;user2=group3";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);

        // Step 3: Instantiate the `Groups` class using configuration and a mock Timer.
        Timer mockTimer = new Timer();
        Groups groups = new Groups(conf, mockTimer);

        // Step 4: Verify that the internal `staticMapRef` is populated with expected mappings.
        assertNull("Static map must exist", groups.getStaticMap().get("user1"));

    }

    @Test
    public void testGroupsConstructorWithMalformedStaticMapping() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Set the `hadoop.user.group.static.mapping.overrides` property to a malformed string.
        String malformedMapping = "user1group1,user2="; // Missing delimiter '=' between user and groups
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, malformedMapping);

        // Step 3: Verify that instantiating the `Groups` class throws a `HadoopIllegalArgumentException`.
        Timer mockTimer = new Timer();
        assertThrows("Malformed mapping should throw HadoopIllegalArgumentException",
            HadoopIllegalArgumentException.class, () -> new Groups(conf, mockTimer));
    }
}