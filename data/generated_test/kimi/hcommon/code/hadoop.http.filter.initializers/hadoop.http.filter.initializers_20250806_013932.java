package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

public class HttpServer2ConfigTest {

    private static final String HTTP_FILTER_INITIALIZER_PROPERTY =
            "hadoop.http.filter.initializers";

    @Test
    public void testMalformedClassNameThrowsException() {
        Configuration conf = new Configuration(false);
        conf.set(HTTP_FILTER_INITIALIZER_PROPERTY,
                 "com.example.NonExistentFilterInit");

        try {
            new HttpServer2.Builder()
                    .setName("test")
                    .setConf(conf)
                    .addEndpoint(URI.create("http://localhost:0"))
                    .build();
            fail("Expected RuntimeException due to non-existent class");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        } catch (IOException e) {
            fail("Unexpected IOException: " + e);
        } catch (Exception e) {
            fail("Unexpected Exception: " + e);
        }
    }

    @Test
    public void testValidSingleFilterInitializer() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(HTTP_FILTER_INITIALIZER_PROPERTY,
                 StaticUserWebFilter.class.getName());

        HttpServer2 server = null;
        try {
            server = new HttpServer2.Builder()
                    .setName("test")
                    .setConf(conf)
                    .addEndpoint(URI.create("http://localhost:0"))
                    .build();
            assertNotNull(server);
        } finally {
            if (server != null) server.stop();
        }
    }

    @Test
    public void testValidMultipleFilterInitializers() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(HTTP_FILTER_INITIALIZER_PROPERTY,
                 StaticUserWebFilter.class.getName() + "," +
                 StaticUserWebFilter.class.getName());

        HttpServer2 server = null;
        try {
            server = new HttpServer2.Builder()
                    .setName("test")
                    .setConf(conf)
                    .addEndpoint(URI.create("http://localhost:0"))
                    .build();
            assertNotNull(server);
        } finally {
            if (server != null) server.stop();
        }
    }

    @Test
    public void testInvalidClassTypeFilterInitializer() {
        Configuration conf = new Configuration(false);
        conf.set(HTTP_FILTER_INITIALIZER_PROPERTY,
                 java.lang.String.class.getName());

        try {
            new HttpServer2.Builder()
                    .setName("test")
                    .setConf(conf)
                    .addEndpoint(URI.create("http://localhost:0"))
                    .build();
            fail("Expected RuntimeException due to invalid class type");
        } catch (RuntimeException e) {
            // In 2.8.5, the exception is not wrapped as ClassCastException
            // but as a RuntimeException with a message indicating the class
            // cannot be cast to FilterInitializer. Adjusting the assertion.
            assertTrue(e.getMessage().contains("cannot be cast to"));
        } catch (IOException e) {
            fail("Unexpected IOException: " + e);
        } catch (Exception e) {
            fail("Unexpected Exception: " + e);
        }
    }

    @Test
    public void testEmptyFilterInitializer() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(HTTP_FILTER_INITIALIZER_PROPERTY, "");

        HttpServer2 server = null;
        try {
            server = new HttpServer2.Builder()
                    .setName("test")
                    .setConf(conf)
                    .addEndpoint(URI.create("http://localhost:0"))
                    .build();
            assertNotNull(server);
        } finally {
            if (server != null) server.stop();
        }
    }

    @Test
    public void testNullFilterInitializer() throws Exception {
        Configuration conf = new Configuration(false);

        HttpServer2 server = null;
        try {
            server = new HttpServer2.Builder()
                    .setName("test")
                    .setConf(conf)
                    .addEndpoint(URI.create("http://localhost:0"))
                    .build();
            assertNotNull(server);
        } finally {
            if (server != null) server.stop();
        }
    }
}