package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import org.junit.*;
import static org.junit.Assert.*;
import java.util.HashMap;

public class NewHuffmanTest {
    private NewHuffman h;

    @Before
     public void setUp() {
	h = new NewHuffman();
     }
 
    @After
    public void tearDown() {
	h.clear();
    }

    @Test
    public void leafTest() {
	long lt = 1000000;
	// clear the indexBit
	lt = lt & (~ (1 << 63));
	int freq = (int) (lt >> 32);
	int value = (int) lt;
	long l = h.leaf(freq, value);
	System.out.println("leafTest: l = " + l + " lt = " + lt);
	assertEquals(l, lt);
	int v = h.readValue(l);
	assertEquals(v, value);
	int f = h.readFreq(l);
	assertEquals(f, freq);
    }

    @Test
    public void forkTest() {
	long lt = 1000000;
	// set the indexBit
	lt = lt | (1L << 63);
	System.out.println(Long.toBinaryString(1L << 63));
	int freq = ((int) (lt >>> 32)) & (~ (1 << 31));
	int left = (((int) lt) & (0xFFFF << 16)) >>> 16;
	int right = ((int) lt) & 0xFFFF;
	System.out.println(Integer.toBinaryString(right));
	long l = h.fork(freq, left, right);
	System.out.println("forkTest: l = " + l + " lt = " + lt);
	assertEquals(l, lt);
	int li = h.readLeftIndex(l);
	assertEquals("readLeftIndex() broken", left, li);
	int ri = h.readRightIndex(l);
	assertEquals("readRightIndex() broken", right, ri);
	int f = h.readFreq(l);
	assertEquals(f, freq);
    }

    @Test
    public void encodeTest() {
	int values[] = new int[] {1, 2, 3, 4, 5, 6,7, 8,9};
	int freq[] = new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1};

	for (int i = 0; i < freq.length; i++) {
	    h.add(freq[i], values[i]);
	}

	h.encode();
	h.compact();

	HashMap<Integer, Long> m = h.visitCodes();
	for (int v : values) {
	    long c = m.get(v);
	    System.out.println(v + " : " + Long.toBinaryString(c));
	}	
    }
	
}
