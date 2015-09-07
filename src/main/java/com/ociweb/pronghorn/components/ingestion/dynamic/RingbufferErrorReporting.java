package com.ociweb.pronghorn.components.ingestion.dynamic;

import com.ociweb.pronghorn.pipe.Pipe;

@Deprecated //DELETE BEHAVIOR PROVIDED BY GraphManager
public class RingbufferErrorReporting {

	final Pipe[] rings;
	
	public RingbufferErrorReporting(Pipe ... rings) {
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
