package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;       
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestGroups {

    private Groups groups;
    private Configuration conf;

    // Prepare the input conditions for unit testing.
    @Before
    public void setUp() throws Exception {
        // Initialize the configuration and set it up using API
        conf = new Configuration();

        // Instantiate the Groups object and mock implementations 
        groups = spy(new Groups(conf));
    }

    @Test
    public void test_getGroups_returnsCorrectGroups() throws IOException {
          // Catch Invalid
        try 
        {
        }
    }