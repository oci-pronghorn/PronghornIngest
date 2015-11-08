package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeBundle;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

@Deprecated //found in another project both newer and better
public class FASTDecodeStage extends PronghornStage {

	private Pipe inputRing;
	private Pipe outputRing;
	private CatByteProvider cbp;

	private FASTReaderReactor reactor;
	
	private long messageCount;
	
	public FASTDecodeStage(GraphManager gm, Pipe inputRingBuffer, CatByteProvider cbp, Pipe outputRingBuffer) {
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
			Pipe.shutdown(inputRing);
			Pipe.shutdown(outputRing);
		}
	}
	
	
	private static FASTReaderReactor reactor(FASTDecodeStage fds) {
		
		 
		if (null == fds.reactor) {
			
			//as the data is decoded it COULD be written to different ring buffers TODO: this split out function is not used at the moment.
			PipeBundle rbs = PipeBundle.buildRingBuffers(fds.outputRing);
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
		 Pipe rb = reactor.ringBuffers()[0];
		
        while (FASTReaderReactor.pump(reactor)>=0) { //continue if there is no room or if a fragment is read.
            PipeReader.tryReadFragment(rb);

            if (PipeReader.isNewMessage(rb)) {
                int msgIdx = PipeReader.getMsgIdx(rb);
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
