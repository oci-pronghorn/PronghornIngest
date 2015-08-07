package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import java.util.HashMap;
import java.util.Map;


public abstract class HuffmanTree implements Comparable<HuffmanTree> {
	public final int freq;

	
	public Map<Long,Long> validationMap = new HashMap<Long,Long>();
	
	public HuffmanTree(int freq) {
		this.freq = freq;
	}

	public int compareTo(HuffmanTree that) {
		return Integer.compare(this.freq, that.freq);
	}
		
	@Override
	public int hashCode() {
		return freq;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof HuffmanTree) {
			HuffmanTree that = (HuffmanTree)obj;
			return that.freq == this.freq;			
		} else {
			return false;
		}
	}

	public String toString() {
		return "Node freq:"+freq;
	}
	
	public abstract void visitCodes(HuffmanVisitor visitor, long code);
	
}

class HuffmanLeaf extends HuffmanTree {

	private final int value;

	public HuffmanLeaf(int freq, int value) {
		super(freq);
		this.value = value;
	}
	
    public void visitCodes(HuffmanVisitor visitor, long code) {
    	assert(0!=code);
    	visitor.visit(value, freq, code);        
    }
}

class HuffmanNode extends HuffmanTree {
	private final HuffmanTree left, right;

	public HuffmanNode(HuffmanTree left, HuffmanTree right) {
		super(left.freq + right.freq);
		this.left = left;
		this.right = right;
	}
	
    public void visitCodes(HuffmanVisitor visitor, long code) {
    	assert(0!=code);
    	long tmp = code<<1;
        left.visitCodes(visitor, tmp);
        right.visitCodes(visitor, tmp | 1);
    }
}
