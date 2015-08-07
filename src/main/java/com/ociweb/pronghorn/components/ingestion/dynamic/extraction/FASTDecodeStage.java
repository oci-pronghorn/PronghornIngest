package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FASTDecodeStage extends PronghornStage {

	private RingBuffer inputRing;
	private RingBuffer outputRing;
	private CatByteProvider cbp;

	private FASTReaderReactor reactor;
	
	private long messageCount;
	
	public FASTDecodeStage(GraphManager gm, RingBuffer inputRingBuffer, CatByteProvider cbp, RingBuffer outputRingBuffer) {
		super(gm, inputRingBuffer, outputRingBuffer);
		this.inputRing  = inputRingBuffer;  // raw byte stream
		this.outputRing = outputRingBuffer; // structured record stream
		this.cbp = cbp;
	}
	
	@Override
	public void run() {
		try {						
			while (decodeUntilRingEmpty(this)) {
				Thread.yield();
			}
		} catch (Throwable t) {	
			t.printStackTrace();
			RingBuffer.shutdown(inputRing);
			RingBuffer.shutdown(outputRing);
		}
	}
	
	
	private static FASTReaderReactor reactor(FASTDecodeStage fds) {
		
		 
		if (null == fds.reactor) {
			
			//as the data is decoded it COULD be written to different ring buffers TODO: this split out function is not used at the moment.
			RingBuffers rbs = RingBuffers.buildRingBuffers(fds.outputRing);
			FASTDecoder readerDispatch;
			boolean debug = true;
			if (debug) {
				readerDispatch = DispatchLoader.loadDispatchReaderDebug(fds.cbp.getCatBytes(),rbs); 
			} else {
				readerDispatch = DispatchLoader.loadDispatchReader(fds.cbp.getCatBytes(),rbs); 
			}
			System.err.println("using: "+readerDispatch.getClass().getSimpleName());
			
		
			
			PrimitiveReader reader = new PrimitiveReader(65536, new FASTInputStream(new RingInputStream(fds.inputRing)),32);		
			fds.reactor = new FASTReaderReactor(readerDispatch,reader);

		}
		return fds.reactor;
	}
	
	
	public static boolean decodeUntilRingEmpty(FASTDecodeStage fds) {	
		
		FASTReaderReactor reactor = reactor(fds);
		 RingBuffer rb = reactor.ringBuffers()[0];
		
        while (FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
            RingReader.tryReadFragment(rb);

            if (RingReader.isNewMessage(rb)) {
                int msgIdx = RingReader.getMsgIdx(rb);
                if (msgIdx<0) {
                	//push last EOF message
        			
//        			long target = RingBuffer.headPosition(outputRing) -  (1+outputRing.mask-2);
//        			RingBuffer.spinBlockOnTail(RingBuffer.getWorkingTailPosition(outputRing), target,outputRing);
//        		
//        			RingBuffer.addNullByteArray(outputRing);
//        			RingBuffer.publishWrites(outputRing);
//        			
//        			flush(this);
                	return false;
                }
                
                fds.messageCount++;
                System.err.println(fds.messageCount);
            }
        }
        
        return true;
	}
	

}
