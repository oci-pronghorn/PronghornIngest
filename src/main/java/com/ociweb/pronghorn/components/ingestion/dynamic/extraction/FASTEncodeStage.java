package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputRingBuffer;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


//TODO: once this works move it to the jFAST project it does not belong here.
public class FASTEncodeStage extends PronghornStage {
	
	private final int WRITE_BUFFER_SIZE = 4095; //No single fragment can be bigger than this
	private FASTDynamicWriter dynamicWriter;
	private PrimitiveWriter writer;
	
	private RingBuffer inputRing;
	private RingBuffer outputRing;
	private CatByteProvider cbp;

	public FASTEncodeStage(GraphManager gm, RingBuffer inputRingBuffer, CatByteProvider cbp, RingBuffer outputRingBuffer)  {
		super(gm,inputRingBuffer,outputRingBuffer);
		this.inputRing  = inputRingBuffer;  // structured record stream
		this.outputRing = outputRingBuffer; // raw byte stream
		this.cbp = cbp;
		
	}
	
	public static void encodeUntilRingEmpty(FASTEncodeStage fes) {	
		//Needed for migration to full ring, this will be null when we are using an external thread
		if (null!=fes) {
			FASTDynamicWriter dynamicWriter = dynamicWriter(fes);

			   long tmp = RingBuffer.getWorkingHeadPositionObject(fes.inputRing).value & fes.inputRing.mask;
			
				while (RingReader.tryReadFragment(fes.inputRing)) {
				
					if (RingReader.isNewMessage(fes.inputRing) &&
						RingReader.getMsgIdx(fes.inputRing)==-1) {	
						
						PrimitiveWriter.assertAllFlushed(fes.writer);
						PrimitiveWriter.flush(fes.writer);
						
	        			while (!RingBuffer.roomToLowLevelWrite(fes.outputRing, RingBuffer.EOF_SIZE)) {	        				
	        			}	        			
	        			RingBuffer.publishEOF(fes.outputRing);
						
	        			RingBuffer.setReleaseBatchSize(fes.inputRing, 0);
	        			RingReader.releaseReadLock(fes.inputRing);	        			
	        			
	        			fes.requestShutdown();
						return;
					}
					
					
					try {
						FASTDynamicWriter.write(dynamicWriter);
					} catch (Throwable t) {
						System.err.println("pos is:"+ tmp+ " is new msg:"+RingReader.isNewMessage(fes.inputRing)+"  msgIdx:"+RingReader.getMsgIdx(fes.inputRing));
						throw t;
					}
					
					
		        }
			
		}
		return;
	}
	
	public static void flush(FASTEncodeStage fes) {	
		if (null!=fes) {
			PrimitiveWriter.flush(fes.writer);
		}
	}

	@Override
	public void run() {

		encodeUntilRingEmpty(this);
		flush(this);	

	}


	private static FASTDynamicWriter dynamicWriter(FASTEncodeStage fes) {
		if (null == fes.dynamicWriter) {
			
			FASTEncoder writerDispatch;
			boolean debug = true;
			if (debug) {
				writerDispatch = DispatchLoader.loadDispatchWriterDebug(fes.cbp.getCatBytes()); 
			} else {
				writerDispatch = DispatchLoader.loadDispatchWriter(fes.cbp.getCatBytes()); 
			}
			
			System.err.println("using: "+writerDispatch.getClass().getSimpleName());
			//TODO: T, should assert that catBytes match outputRing FROM.
			
			fes.writer = new PrimitiveWriter(fes.WRITE_BUFFER_SIZE, new FASTOutputRingBuffer(fes.outputRing), false);
			fes.dynamicWriter = new FASTDynamicWriter(fes.writer, fes.inputRing, writerDispatch);
		}
		return fes.dynamicWriter;
	}

}
