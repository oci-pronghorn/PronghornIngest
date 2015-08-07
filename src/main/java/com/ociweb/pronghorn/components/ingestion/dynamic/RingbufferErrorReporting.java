package com.ociweb.pronghorn.components.ingestion.dynamic;

import com.ociweb.pronghorn.ring.RingBuffer;

@Deprecated //DELETE BEHAVIOR PROVIDED BY GraphManager
public class RingbufferErrorReporting {

	final RingBuffer[] rings;
	
	public RingbufferErrorReporting(RingBuffer ... rings) {
		this.rings = rings;
	}
	
	public void report() {
		int i = 0;
		while (i<rings.length) {
			
			System.err.println("ring "+i+" "+rings[i].toString());
			i++;
		}
		
	}

}
