package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpServer2FilterInitializerTest {

    @Test
    public void testNonFilterInitializerSubclassThrowsException() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration(false);

        // 2. Prepare the test conditions.
        conf.set("hadoop.http.filter.initializers", "org.apache.hadoop.http.HttpServer2FilterInitializerTest$NotAFilterInit");

        // 3. Test code.
        HttpServer2.Builder builder = new HttpServer2.Builder();
        builder.setName("test").setConf(conf).setFindPort(true).addEndpoint(new URI("http://localhost:0"));
        try {
            builder.build();
            fail("Expected ClassCastException");
        } catch (ClassCastException e) {
            // 4. Code after testing.
            assertEquals("org.apache.hadoop.http.HttpServer2FilterInitializerTest$NotAFilterInit cannot be cast to org.apache.hadoop.http.FilterInitializer",
                    e.getMessage());
        } catch (IOException e) {
            // Handle other potential exceptions from build()
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    public static class NotAFilterInit {
        // Dummy class that does not extend FilterInitializer
    }
}