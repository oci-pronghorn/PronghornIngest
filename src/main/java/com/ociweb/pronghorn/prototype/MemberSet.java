package com.ociweb.pronghorn.prototype;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import com.ociweb.pronghorn.ring.util.hash.MurmurHash;

public class MemberSet {

	//TODO: refactor to make this a top level feature
	public static void main(String[] arg) {
		
		/**
		 * Send 200,000 bad random numbers originally.
		 * Takes 4MB to send for first xmit
		 * 
		 * Then we add 1,000 additional random numbers.
		 * This would require another 4MB xmit 
		 * But with this new technique it is done with 
		 * a transmit of 62K or .062MB or 1/66 or 98.48% compression 
		 * this is only for additions.
		 * 
		 * Further research is required for list removals but it also possible using this technique.
		 * 
		 * Note: if we use the bloom on the client side to find the new cards then
		 * only the list of 1000 which takes 8000 would need to be sent. they can do there own hash on the other end.
		 * This is 8K send or .008MB or 1/512 or 99.8% compression.
		 * 
		 * Each node keeps a hash and only passes on new bad values.
		 * 
		 * 
		 * TODO: extract this into a utility method kept in util of ring.
		 * 
		 * 
		 * TODO: new product development
		 *       scan text fields placed on the byte ring buffer with a running hash that is reset when space is encounted.
		 *       If the hash matches a bloom filter route it to a bucket.
		 *       OR keep counting bloom filter. Then flag how similar a document.
		 * 
		 * 
		 */
		
		
		int seed = 42;
		int size = 300000;
		int bloomBits = 25;//32M bits
		int bloomMask = (1<<bloomBits)-1;
		int bloomIntSize = 1<<(bloomBits-3);
		int k = 66;
		//http://hur.st/bloomfilter?n=300000&p=1.0E-20
		//n = 300000
		//p = 1.0E-20
		//k = 66  hash functions
		//m = 28,755,176 bits in filter
		int additional     = 1000;
		int additionalSeed = 999;
		
		
		byte[] bloom = new byte[bloomIntSize];
		int[] bloomSeeds = buildSeeds(k,777);

		
		long[] data = buildTestData(size,seed);		
		populateBloom(bloomMask, bloom, data, bloomSeeds);
		
		validateBloom(bloom,data,bloomSeeds,bloomMask);
						
		int compSize = compSize(bloom);
		
		long rawASCIISize = computeRawSize(data);
		System.err.println("  original size: "+rawASCIISize);
		System.err.println("     bloom size: "+bloomIntSize);
		System.err.println("compressed size: "+compSize );
		

		
		long[] additionalData = buildTestData(additional,additionalSeed);
		long additionalRawASCIISize = computeRawSize(additionalData);
		byte[] additionalBloom = new byte[bloomIntSize];
		
		populateBloom(bloomMask, additionalBloom, additionalData, bloomSeeds);
		int additionalCompSize = compSize(additionalBloom);
		System.err.println(" add 1     size: "+additionalCompSize );
		
		// A   B   ?   ^  &  |
		// 1   1   0   0  1  1
		// 1   0   0   1  0  1
		// 0   1   1   1  0  1
		// 0   0   0   0  0  0
		//  (a^b)&b
		
		byte[] dif = buildDif(bloom,additionalBloom);
		
		int difcompSize = compSize(dif);
		System.err.println(" add 2     size: "+difcompSize );
		long fullXmitWithAdditions = additionalRawASCIISize + rawASCIISize;
		System.err.println("full update    : "+fullXmitWithAdditions);
		float pct = 100f*(1f-(difcompSize/(float)fullXmitWithAdditions));
		System.err.println(pct+"% compression over full send");
		
		//confirm these two lists share no common values
		checkForOverlap(bloomMask, bloom, bloomSeeds, additionalData);
		checkForOverlap(bloomMask, additionalBloom, bloomSeeds, data);
		
		
				
		//new test showing constant time extraction of new members.
		int joinSeed = 23;
		long[] joinData = randomJoin(data, additionalData, joinSeed);
		int expectedLength = joinData.length-data.length;
						
		long[] rebuiltAdditions = extractNewMembers(bloomMask, bloom, bloomSeeds, joinData, expectedLength);
		
		if (!Arrays.equals(rebuiltAdditions, additionalData)) {
			System.err.println("WARNING: rebuld collection is not the same");
		}

		int sendNewOnlySize = rebuiltAdditions.length*8;
		float newPct = 100f*(1f-(sendNewOnlySize/(float)fullXmitWithAdditions));
		System.err.println(newPct+"% compression with extract new members. "+  (100f/(100f-newPct))+" times" );
		
	}



	public static long[] extractNewMembers(int bloomMask, byte[] bloom,
			int[] bloomSeeds, long[] joinData, int expectedLength) {
		long[] rebuiltAdditions = new long[expectedLength];
		int r = rebuiltAdditions.length;
		int i = joinData.length;
		while (--i>=0) {			
			if (!isFound(joinData[i],bloom,bloomSeeds,bloomMask)) {
				rebuiltAdditions[--r] = joinData[i];
			} 
		}
		return rebuiltAdditions;
	}



	public static void checkForOverlap(int bloomMask, byte[] bloom,
			int[] bloomSeeds, long[] additionalData) {
		
		int w = additionalData.length;
		int count = 0;
		while (--w>=0) {
			if (!isFound(additionalData[w],bloom,bloomSeeds,bloomMask)) {
				count++;
			} else {
				System.err.println("found value "+additionalData[w]+" at "+w);
			}
		}
		if (count!=additionalData.length) {
			System.err.println("WARNING: overlap found "+count+" vs "+additionalData.length);
		}
	}



	private static byte[] buildDif(byte[] bloom, byte[] additionalBloom) {
		int j = bloom.length;
		byte[] result = new byte[j];
		while (--j>=0) {
			result[j] = (byte)(0xFF&(bloom[j]^additionalBloom[j])&additionalBloom[j]);
		}
		return result;
	}

	public static int compSize(byte[] bloom) {
		
		//TODO: try other compression ? eg how many bits are on where just store the index?
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream zipOut;
		try {
			zipOut = new GZIPOutputStream(baos);
			zipOut.write(bloom);
			zipOut.close();		
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		int compSize = baos.toByteArray().length;
		
		//vs
		
		int j = bloom.length;
		List<Integer> runLengthEncoding = new ArrayList<Integer>();
		int last = 0;
		while (--j>=0) {
			byte b = bloom[j];
			if (b!=0) {
				int i = 8;
				while (--i>=0) {
					if ((b&1)!=0) {
						runLengthEncoding.add((j+i)-last);
						last = j+i;
					}
					b=(byte)(b>>1);
				}
			}			
		}
		int x=0;
		byte[] buffer = new byte[runLengthEncoding.size()*3];
		for(Integer i:runLengthEncoding) {
			//buffer[x++] = (byte)(0xFF&(i>>>24)); //only need 24 bits because that is bigger than the bloom filter
		    buffer[x++] = (byte)(0xFF&(i>>>16));
			buffer[x++] = (byte)(0xFF&(i>>>8));
			buffer[x++] = (byte)(0xFF&i);
		}
		ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
		GZIPOutputStream zipOut2;
		try {
			zipOut2 = new GZIPOutputStream(baos2);
			zipOut2.write(buffer);
			zipOut2.close();		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int compSize2 = baos2.toByteArray().length;
		
		//NOTE: Based on how dense the bloom filter is we pick the most optmizl methodolgoy, need to send leading byte to tell decoder.
		System.err.println(compSize+" vs "+compSize2);
		return Math.min(compSize,compSize2);
		
		
		
		//return compSize;
	}
	
	public static long[] randomJoin(long[] a, long[] b, int seed) {
		long[] response = new long[a.length+b.length];

		Random r = new Random(seed);
		
		int aIdx = a.length;
		int bIdx = b.length;
		
		int i = response.length;
		while (--i>=0) {
			response[i] = r.nextBoolean()&(aIdx>0)|(bIdx==0) ? a[--aIdx] : b[--bIdx];
		}
		return response;
	}

	public static void populateBloom(int bloomMask, byte[] bloom, long[] data,
			int[] bloomSeeds) {
		int j = data.length;
		while (--j>=0) {
			long value = data[j];
			int i = bloomSeeds.length;
			while (--i>=0) {
				int h = hash(value,bloomSeeds[i])&bloomMask;
				int idx = h>>3;
				int val = 1<<(h&0x7); //values 0 to 7
				bloom[idx] = (byte)(bloom[idx] | val);
			}
		}
	}
	
	
	private static void validateBloom(byte[] bloom, long[] data, int[] bloomSeeds, int bloomMask) {
		int j = data.length;
		while (--j>=0) {
			if (!isFound(data[j], bloom, bloomSeeds, bloomMask)) {
				throw new RuntimeException("can not find");
			}
		}		
		//TODO: add check for expected probability failure of 10^20
	}
	
	private static boolean isFound(long value, byte[] bloom, int[] bloomSeeds, int bloomMask) {

		int i = bloomSeeds.length;
		while (--i>=0) {
			int h = hash(value,bloomSeeds[i])&bloomMask;
			int idx = h>>3;
			int val = 1<<(h&0x7); //values 0 to 7
			if (0 == (bloom[idx]&val)) {
				return false;
			}
		}
		return true;
	}



	private static int[] buildSeeds(int k, int masterSeed) {
		
		Random r = new Random(masterSeed);
		
		int[] result = new int[k];
		
		int j = k;
		while (--j>=0) {
			result[j] =	r.nextInt();
		}
		return result;
	}

	public static int hash(long value, int seed) {
		byte[] src = new byte[8];
		int j = 8;
		while (--j>=0) {			
			src[j] = (byte)(0xFF*value);
			value = value>>8;
		}		
		
		return MurmurHash.hash32(src, 0, 8, seed);
				
	}
	
	private static long computeRawSize(long[] data) {
		
		long result = 0;
		int j = data.length;
		while (--j>=0) {
			result +=  Long.toString(data[j]).length();
			result++; //for new line
		}
		return result;
	}



	public static long[] buildTestData(int size, int seed) {
		
		Random r = new Random(seed);
		
		long[] result = new long[size];
		
		int j = size;
		while (--j>=0) {
			result[j] =	r.nextLong();
		}
		return result;
	}
	
	
	
	
}
