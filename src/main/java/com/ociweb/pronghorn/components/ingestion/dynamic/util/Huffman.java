package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Huffman {
	
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
	
	
	private final List<HuffmanTree> trees = new ArrayList<HuffmanTree>();
	private final Set<Integer> validation = new HashSet<Integer>();
	
	
	public static void add(int value, int freq, Huffman h) {
		if (h.validation.contains(value)) {
			//TODO: due to the mask RecordFieldExtractor 1607 we need to sum the freq together not throw an error, this sum must be above this somewhere.
			return;
			//throw new RuntimeException("Only unique values are supported already found "+value);
		}
		h.validation.add(value);
		h.trees.add(new HuffmanLeaf(freq,value));
	}
	    
    public static HuffmanTree encode(Huffman h) {
    	
    	Collections.sort(h.trees);
        
    	while (h.trees.size() > 1) {
            HuffmanTree a = h.trees.remove(0);
            HuffmanTree b = h.trees.remove(0);
            
    		h.trees.add(new HuffmanNode(a,b));
    		Collections.sort(h.trees);
                		
        }
        return h.trees.remove(0);
    }
    
    
}
