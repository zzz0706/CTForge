package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

//HBASE-17392
@Category(SmallTests.class)
public class TestHStoreEngineClassConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestHStoreEngineClassConfig.class);

    @Test
    public void testHStoreEngineClassConfig() {
        Configuration conf = new Configuration();
        String className = conf.get("hbase.hstore.engine.class", "").trim();

        // Allow unset (empty): should use default
        if (className.isEmpty())
            return;

        boolean valid = false;
        try {
            Class<?> clazz = Class.forName(className);
            valid = clazz != null;
        } catch (ClassNotFoundException e) {
            valid = false;
        }

        assertTrue(
                "hbase.hstore.engine.class must be a valid, loadable class. Found: " + className,
                valid);
    }
}
