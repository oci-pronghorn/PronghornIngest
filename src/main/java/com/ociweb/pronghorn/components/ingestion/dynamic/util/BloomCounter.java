package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.util.Histogram;

public class BloomCounter {

	public final int[] bloom;
	private final int seed;
	private final int size;
	
	public final int mask;
	public final int id;
	public final int bits;
	
	private Histogram stats = null;
	private int uniqueCount;
	
	public BloomCounter(int sizeBits, int seed, int id) {
		this.bits = sizeBits;
		this.size = 1<<sizeBits;
		this.mask = size-1;
		
		this.id = id;
		this.bloom = new int[size];
		this.seed = seed;
	}
	
	public static int hash(ByteBuffer src, int offset, int length, BloomCounter target) {
		return MurmurHash.hash32(src, offset, length, target.seed);
	}
	
	
	public static void sample(byte[] src, int offset, int length, BloomCounter target) {
		target.bloom[target.mask&MurmurHash.hash32(src, offset, length, target.seed)]++;
	}
	
	
	public static void sample(ByteBuffer src, int offset, int length, BloomCounter target) {
		target.bloom[target.mask&MurmurHash.hash32(src, offset, length, target.seed)]++;
	}
	
	public static void sample(int hash, BloomCounter target) {		
		target.bloom[hash&target.mask]++;
	}
	

	
	
	public static void buildSummary(BloomCounter target) {
		
		int max = 0;
		int j = target.size;
		while (--j>=0) {
			int sum = target.bloom[j];
			if (sum>max) {
				max = sum;
			}
		}
		
		target.stats = new Histogram(10000,10,0,max);
		
		int uniqueCount = 0;
		j = target.size;
		while (--j>=0) {
			int sum = target.bloom[j];
			Histogram.sample((long) sum, target.stats);
			if (sum>0) {
				uniqueCount++;
			}
		}
		target.uniqueCount = uniqueCount;
		
		boolean debug = false;
		if (debug) {
			System.err.println("estimated:"+uniqueCount+" values");
			System.err.println(target.stats);
		}
	}
	
	public static int uniqueCount(BloomCounter target) {
		return target.uniqueCount;
	}
	
	public static Histogram stats(BloomCounter target) {
		return target.stats;
	}
	
	public static void visitAbovePercentile(double percentileFilter, BloomCountVisitor visitor, BloomCounter target) {
		
		buildSummary(target);		
		
		long threshold = target.stats.valueAtPercent(percentileFilter);

		int hash = target.size;
		while (--hash>=0) {
			int sum = target.bloom[hash];
			if (sum > threshold) {
			    visitor.visit(hash, sum );
			} 			
		}
	}	
	
	
	
}
