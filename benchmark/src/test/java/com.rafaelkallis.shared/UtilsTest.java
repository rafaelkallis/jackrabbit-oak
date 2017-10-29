package com.rafaelkallis.shared;

import junit.framework.TestCase;

import static com.rafaelkallis.shared.Utils.*;

public class UtilsTest extends TestCase {

    public void testFirstNode() throws Exception {
        assertEquals(0, firstNode(0));
        assertEquals(1, firstNode(1));
        assertEquals(3, firstNode(2));

        assertEquals(0, firstNode(0, 2));
        assertEquals(1, firstNode(1, 2));
        assertEquals(3, firstNode(2, 2));
        assertEquals(4, firstNode(2, 3));
        assertEquals(5, firstNode(2, 4));
    }

    public void testLastNode() throws Exception {
        assertEquals(0, lastNode(1));
        assertEquals(2, lastNode(2));
        assertEquals(6, lastNode(3));

        assertEquals(0, lastNode(1, 2));
        assertEquals(2, lastNode(2, 2));
        assertEquals(6, lastNode(3, 2));
        assertEquals(3, lastNode(2, 3));
        assertEquals(12, lastNode(3, 3));
    }

    public void testTotalNodes() throws Exception {
        assertEquals(7, totalNodes(0, 3));
        assertEquals(6, totalNodes(1, 3));

        assertEquals(7, totalNodes(0, 3, 2));
        assertEquals(6, totalNodes(1, 3, 2));
        assertEquals(13, totalNodes(0, 3, 3));
        assertEquals(12, totalNodes(1, 3, 3));
    }

    public void testMapToPath() throws Exception {
        assertEquals("", mapToPath(0));
        assertEquals("a/", mapToPath(1));
        assertEquals("b/", mapToPath(2));
        assertEquals("a/a/", mapToPath(3));
        assertEquals("a/b/", mapToPath(4));
        assertEquals("b/a/", mapToPath(5));
        assertEquals("b/b/", mapToPath(6));
    }
}