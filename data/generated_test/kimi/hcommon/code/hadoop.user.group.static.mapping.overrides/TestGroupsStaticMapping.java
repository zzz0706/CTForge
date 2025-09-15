package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestGroupsStaticMapping {

    @Test
    public void verifyMalformedMappingThrowsException() {
        Configuration conf = new Configuration();
        conf.set("hadoop.user.group.static.mapping.overrides",
                 "user1=group1=extra;user2");

        try {
            new Groups(conf, new Timer());
            fail("Expected IllegalArgumentException for malformed mapping");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}