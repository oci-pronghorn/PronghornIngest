package com.ociweb.pronghorn.components.ingestion.dynamic.stage;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;

/**Only needed for new dynmaic ingest behavior, THIS IS NOT THE NORMAL CASE BUT WILL BE REQUIRED FOR FULL FLEXIBILITY */
public class TemplateLoaderStage {
	RingInputStream input;
	
	/** Given an input ring with Catalog Template XML produce a TemplateCatalog object ??*/
	TemplateLoaderStage(RingBuffer input) {
		this.input = new RingInputStream(input);
	}
	
	public void run() {
	
		//TODO: New design for this loader stage.
		//Dynamic stream consumer will take two input streams, one for these catBytes and one for the records.
		//Cat byte message will need its own template and must contain count of records that are applicable.
		
		
	}
	
}
