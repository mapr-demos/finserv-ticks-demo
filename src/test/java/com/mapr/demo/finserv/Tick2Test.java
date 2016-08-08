package com.mapr.demo.finserv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class Tick2Test {

    @org.junit.Test
    public void testGetDate() throws IOException {
        List<String> data = Resources.readLines(Resources.getResource("sample-tick-01.txt"), Charsets.ISO_8859_1);
        Tick2 t = new Tick2(data.get(0));
        assertEquals(t.getDate(), "080845201");

        ObjectMapper mapper = new ObjectMapper();
        System.out.printf("%s\n", mapper.writeValueAsString(t));
    }
}