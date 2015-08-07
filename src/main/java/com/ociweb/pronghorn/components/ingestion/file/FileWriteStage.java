package com.ociweb.pronghorn.components.ingestion.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileWriteStage extends PronghornStage {//Moved to meta.

	final RingBuffer input;
	final File target;
	
	public FileWriteStage(GraphManager gm, RingBuffer input, File target) {
		super(gm, input, NONE);
		this.input = input;
		this.target = target;
	}

	@Override
	public void run() {
		
		//TODO: AA, this will be much much faster by using NIO and writing directly from large blocks of the byte buffer.
		//walk all blocks until we reach the head or there is a gap
		// then do block write of those arrays into the target file.
				
		try {
     		OutputStream output = new FileOutputStream(target);
			RingStreams.writeToOutputStream(input, output);
			output.close();
		} catch (IOException e) {
			throw new RuntimeException(e);			
		}
		
	}
	
	
}
