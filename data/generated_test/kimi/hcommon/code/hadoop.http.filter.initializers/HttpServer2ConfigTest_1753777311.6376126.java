package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2.Builder;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpServer2ConfigTest {

    @Test
    public void testMalformedClassNameThrowsException() {
        // 1. Create a fresh Configuration instance
        Configuration conf = new Configuration(false);

        // 2. Set a non-existent filter-initializer class
        conf.set("hadoop.http.filter.initializers",
                 "com.example.NonExistentFilterInit");

        // 3. Build an HttpServer2 instance; expect RuntimeException
        try {
            new HttpServer2.Builder()
                    .setName("test")
                    .setConf(conf)
                    .addEndpoint(URI.create("http://localhost:0"))
                    .build();
            fail("Expected RuntimeException due to non-existent class");
        } catch (RuntimeException e) {
            // 4. Verify the cause is ClassNotFoundException
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        } catch (IOException e) {
            fail("Unexpected IOException: " + e);
        }
    }
}