package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class NewHuffman {
	
	/**
	 * This is a TODO: Poor huffman encoder and must be replaced, design is here in this comment
	 *    - This one does pointer chasing
	 *    - This one creates garbage
	 *    - This one uses vtable lookup
	 *    - This one does not leverage the cache pre-fetch
	 *    
	 * New design
	 *   - This one does not make use of java pointers just array indexes
	 * 	 - This one does not produce any garbage
	 *   - This one	does not require any vTable lookup
	 *   - This one makes better use of pre-fetch when navigating down the tree.
	 * 
	 *    //Knowing the node count up front is the new requirement for this implementation
	 *    Single array of longs (2*nodeCount)+1  
	 *    
	 *    [] notation is a long
	 *    <> notation is the bits reading them high to low left to right
	 *    
	 *    1. add all pointers to array  [<32 freq><32 value>]
	 *    2. Arrays.sort(yourArray) (done once, structure remains sorted as we compute each iteration)
	 *    3. consume 2 longs off bottom of sorted array
	 *    4. add fork to bottom above last fork, [<32 freqLeft><32 valueLeft/ptr>][<32 freqRight><32 valueRight/prt>]
	 *         value or ptr is determined by high bit (when reading tree back check for <0 should be used)
	 *    5. move array down until we find the insert point and insert pointer to fork <32 totalFreq><32 ptr>     
	 *    6. repeat 3
	 * 
	 */


	/**
	 * layout of a leaf:
	 * 
	 *        32             32
	 * [0------------][--------------]
	 *        ^              ^
	 *      freq           value
	 * 
	 * layout of a fork
	 *        32          16     16
	 * [1------------][--------------]
	 *        ^           ^      ^
	 *       freq       left    right
	 * 
	 * 
	 */
	
	private static final int INDEX_LENGTH 	= 16;
	private static final int FREQ_LENGTH 	= 32;
	private static final int TOTAL_LENGTH 	= 64;
	private static final int DEFAULT_NUM 	= 1024;
	private static final int FREQ_MAX	= 2147483647; // 2^31 - 1
	private static final long INDEX_BIT 	= 1l << (TOTAL_LENGTH - 1);

	private int leafTail = 0;
	private int leafHead = 0;
	private int forkTail = 0;
	private int forkHead = 0;
	private int compactTail = 0;

	private long[] trees;
	
	// TODO: use the embedded hashmap
	private final Set<Integer> validation = new HashSet<Integer>();
	private final int num;

	public NewHuffman() {
		num = DEFAULT_NUM;
		// The total number of fork and leaf in
		// Huffman tree is up to n + (n - 1)
		trees = new long[2 * num];
		forkHead = forkTail = num;
	}
    
	public NewHuffman(int n) {
		num = n;
		trees = new long[2 * num];
		forkHead = forkTail = num;
	}
	
	// Is a index or value in the second part (32 bits)?
	public boolean isIndex(long hi) {
		return (hi & INDEX_BIT) != 0 ? true : false;
	}

	// Create a leaf used by Huffman tree
	public long leaf(int freq, int value) {
	        // freq in (0, 2^31] since the most 
	        // significant bit is used to decide 
	        // index or value
		assert (freq > 0 || freq <= FREQ_MAX) : "Frequncy is out of range";
		return ~INDEX_BIT & ((long) freq) << FREQ_LENGTH | value;
	}

	// Read the value from the leaf
	public int readValue(long lf) {
		assert !isIndex(lf) : "Not a leaf";
		return (int) lf;
	}

	// Create a fork used by Huffman tree
	public long fork(int freq, int li, int ri) {
                // index is only 16 bits
                assert ((li >>> INDEX_LENGTH) == 0 && (ri >>> INDEX_LENGTH) == 0) : "Index is out of range";
		return INDEX_BIT | ((long) freq) << FREQ_LENGTH | ((long) li) << INDEX_LENGTH | (long) ri;
	}

	// Read index of left subtree
	public int readLeftIndex(long lf) {
		assert isIndex(lf) : "is not a fork";
		return ((int) lf) >>> INDEX_LENGTH;
	}

	// Read index of right subtree
	public int readRightIndex(long lf) {
		assert(isIndex(lf)) : "is not a fork";
		return ((int) lf) & (~(0xFFFF << INDEX_LENGTH));
	}

	// Read the frequency from the leaf
	public int readFreq(long lf) {
		lf = lf & (~INDEX_BIT);
		return (int) (lf >> FREQ_LENGTH);
	}

	// Add a (freq, value) pair into Huffman tree
	public void add(int freq, int value) {
		assert noDuplicate(value) : "Only unique values are support";
		validation.add(value);
		assert leafTail < num : "Too much leaf";
		trees[leafTail++] = leaf(freq, value);
	}
    
	// Generate Huffman tree
	public void encode() {
		Arrays.sort(trees, leafHead, leafTail);
		while (!leafEmpty() || !forkEmpty()) {
			// Check the two queues and pick
			// proper element (leaf or fork)
			int left 	= popSmallFreq();
			assert (left != -1) : "Cannot find leaf or fork!";
			int right 	= popSmallFreq();			
			// if there is only one fork, we find the root
			if (right == -1 && isIndex(trees[left])) {
				assert (readFreq(trees[left]) == totalFreq()) : "The root is wrong";
				printTree();
				return;
			} else {
				// if there is only one or more than one leaf, keep building
				// the fork
				int freqSum = readFreq(trees[left])
					+ (right == -1 ? 0 : readFreq(trees[right]));
				trees[forkTail++]	= fork(freqSum, left, right);
			}
		}	    
		assert false : "No huffman tree build";
	}
	
	// compactize encoded Huffman tree
	public void compact() {
		long[] tempTree = new long[2 * num];
		assert forkTail >= 1 : "At least there is one fork";
		long f = trees[forkTail - 1];
		tempTree[compactTail++] = f;
		buildCompactTree(0, tempTree);		
		trees = tempTree;
	}

	// Build compact Huffman tree recursively (pre-ordered)
	public void buildCompactTree(int rootIndex, long[] tempTree) {
		if (!isIndex(tempTree[rootIndex])) {
			return;
		}
		long root = tempTree[rootIndex];
		int left = readLeftIndex(root);
		int leftCompactIndex = compactTail;
		tempTree[compactTail++] = trees[left];
		buildCompactTree(leftCompactIndex, tempTree);
		int right = readRightIndex(root);
		int rightCompactIndex = 0;
		if (right != -1) {
			rightCompactIndex = compactTail;
			tempTree[compactTail++] = trees[right];
			buildCompactTree(rightCompactIndex, tempTree);
		} else {
			rightCompactIndex = -1;
		}
			
		tempTree[rootIndex] = fork(readFreq(root), leftCompactIndex, rightCompactIndex);		
	}

	public HashMap<Integer, Long> visitCodes() {
		assert forkTail >= 1 : "At least there is one fork";
		// no compactization :
		// long f = trees[forkTail - 1];
		long f = trees[0];
		// TODO: replace Java hashmap with customized hashmap
		HashMap<Integer, Long> codes = new HashMap<>();
		visitCodesImp(f, 1L, codes);
		return codes;
	}

	public void visitCodesImp(long root, long code, HashMap<Integer, Long> codes) {
		if (!isIndex(root)) {
			codes.put(readValue(root), code);
			return;
		}
		code <<= 1;
		visitCodesImp(trees[readLeftIndex(root)], code, codes);
		if (readRightIndex(root) != -1) 
			visitCodesImp(trees[readRightIndex(root)], code | 1, codes);
	}			
			

	public int value(long code) {
		assert forkTail >= 1 : "At least there is one fork";
		long f = trees[forkTail - 1];
		int index = 0;
		while (isIndex(f)) {
			index = (code & 1) == 0 ? readLeftIndex(f) : readRightIndex(f);
			if (index == -1) {
				return Integer.MIN_VALUE;
			} else {
				if (!isIndex(trees[index]) 
				    && ((code >>> 1) == 0)) {
					break;
				} else {
					return Integer.MIN_VALUE;
				}
			}
		}		
		return readValue(trees[index]);
	}
	
	// Auxilary function
	private boolean noDuplicate(int v) {
		return !validation.contains(v);
	}
	
	public int popSmallFreq() {
		if (leafEmpty() && forkEmpty()) 
			return -1;

		if (!leafEmpty() && (forkEmpty() | leafSmallFreq())) {
			int indexSmallFreq = leafHead;
			leafHead++;
			return indexSmallFreq;
		} else {
			int indexSmallFreq = forkHead;
			forkHead++;
			return indexSmallFreq;
		}
	}

	public void clear() {
		trees = null;
		validation.clear();    
	} 

	private long totalFreq() {
		long tf = 0;
		for (int i = 0; i < leafTail; i++) {
			tf += readFreq(trees[i]);
		}
		return tf;
	}

	private boolean leafEmpty() {
		return !(leafHead < leafTail);
	}

	private boolean forkEmpty() {
		return !(forkHead < forkTail);
	}

	private boolean leafSmallFreq() {
		return readFreq(trees[leafHead]) < readFreq(trees[forkHead]);
	}
	
	// Print raw format of the tree, only for debug
	private void printTree() {
		for (int i = 0; i < leafTail; ++i) {
			System.out.println("Freq: " + readFreq(trees[i]) 
					   + " Value: " + readValue(trees[i])
					   + " Total: " + trees[i]);
		}
		for (int i = num; i < forkTail; ++i) {
			System.out.println("Freq: " + readFreq(trees[i]) 
					   + " Left: " + readLeftIndex(trees[i])
					   + " Right: " + readRightIndex(trees[i])
					   + " Total: " + trees[i]);
		}
	}
	
}
