package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

//HBASE-5893
@Category(SmallTests.class)
public class TestCoprocessorClassTrim {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestCoprocessorClassTrim.class);

    private static final String CONF_KEY =
        "hbase.coprocessor.regionserver.classes";

    @Test
    public void testCoprocessorClassNamesAreTrimmedAndLoadable() throws Exception {

        Configuration conf = HBaseConfiguration.create();


        // Fetch the split class names
        String[] classes = conf.getStrings(CONF_KEY);
        // Should result in exactly two entries
        assertEquals(2, classes.length);

        for (String clsName : classes) {
            // Each entry should be trimmed (no leading/trailing whitespace)
            assertEquals("Class name should be trimmed", clsName, clsName.trim());
            // And the class should be loadable
            try {
                Class.forName(clsName);
            } catch (ClassNotFoundException e) {
                fail("Expected coprocessor class to be loadable: " + clsName);
            }
        }
    }
}
