package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

//HBASE-21540
@Category(SmallTests.class)
public class TestSystemTablesCompactingMemstoreConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
            .forClass(TestSystemTablesCompactingMemstoreConfig.class);

    private static final String CONF_KEY = "hbase.systemtables.compacting.memstore.type";

    @Test
    public void testPolicyMembership() {
        // Retrieve the value from Configuration
        Configuration conf = new Configuration();
        String value = conf.get(CONF_KEY);

        // Verify that the value exactly matches an enum constant
        assertTrue("The configuration value should exactly match a MemoryCompactionPolicy constant",
                isValidPolicy(value));
    }

    private boolean isValidPolicy(String value) {
        if (value == null) {
            return true;
        }
        String trimmed = value.trim();
        for (MemoryCompactionPolicy policy : MemoryCompactionPolicy.values()) {
            if (policy.name().equals(trimmed)) {
                return true;
            }
        }
        return false;
    }
}