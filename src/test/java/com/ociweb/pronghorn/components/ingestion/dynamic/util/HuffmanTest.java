package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import org.junit.Test;

public class HuffmanTest {

	@Test
	public void huffmanTest() {
		
		int testSize = 11;
		
		int[] values = new int[testSize];
		int[] freqs  = new int[testSize];
		
		int i = testSize;
		while (--i>=0) {
			values[i] = i;
			freqs[i] = i;//%0xFF;			
		}
		
		Huffman h = new Huffman();
		
		int j = values.length;
		while (--j>=0) {
			Huffman.add(values[j], freqs[j],h);
		}
		
		final HuffmanTree tree = Huffman.encode(h);
		
		tree.validationMap.clear();
		HuffmanVisitor visitor = new HuffmanVisitor() {

			@Override
			public void visit(int value, int frequency, long code) {
				
				//This is a unit test failrue!!
				if (tree.validationMap.containsKey(code)) {
					System.err.println("Found a general hash: "+Long.toBinaryString(value)+" "+value
							+"           "+code+"        "+Long.toBinaryString(code));
					System.err.println("Found a general hash: "+Long.toBinaryString(tree.validationMap.get(code))+" "+tree.validationMap.get(code)
							+"           "+code+"        "+Long.toBinaryString(code));
					
				}
				
				tree.validationMap.put(code, (long) value);
				
				System.err.println(value+"   "+frequency+"   "+code+" "+Long.toBinaryString(code));
			}
			
		};
		tree.visitCodes(visitor , 1l);
		
		
		
	}
	
	
}
