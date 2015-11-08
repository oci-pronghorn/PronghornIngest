package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputRingBuffer;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


@Deprecated //found in another project both newer and better
public class FASTEncodeStage extends PronghornStage {
	
	private final int WRITE_BUFFER_SIZE = 4095; //No single fragment can be bigger than this
	private FASTDynamicWriter dynamicWriter;
	private PrimitiveWriter writer;
	
	private Pipe inputRing;
	private Pipe outputRing;
	private CatByteProvider cbp;
	GraphManager gm;

	public FASTEncodeStage(GraphManager gm, Pipe inputRingBuffer, CatByteProvider cbp, Pipe outputRingBuffer)  {
		super(gm,inputRingBuffer,outputRingBuffer);
		this.inputRing  = inputRingBuffer;  // structured record stream
		this.outputRing = outputRingBuffer; // raw byte stream
		this.cbp = cbp;
		this.gm = gm;
		
		dynamicWriter(this);
	}
	
	public static void encodeUntilRingEmpty(FASTEncodeStage fes) {	
	    
	    //NOTE: this stage requires the posion pill in order to shutdown.

	    
		//Needed for migration to full ring, this will be null when we are using an external thread
		if (null!=fes) {
			FASTDynamicWriter dynamicWriter = dynamicWriter(fes);

			   long tmp = Pipe.getWorkingHeadPositionObject(fes.inputRing).value & fes.inputRing.mask;
			
				while (PipeReader.tryReadFragment(fes.inputRing)) {
				
					if (PipeReader.isNewMessage(fes.inputRing) &&
						PipeReader.getMsgIdx(fes.inputRing)==-1) {	
						
						shutdownProcess(fes);
						return;
					}
					
					
					try {
						FASTDynamicWriter.write(dynamicWriter);
					} catch (Throwable t) {
						System.err.println("pos is:"+ tmp+ " is new msg:"+PipeReader.isNewMessage(fes.inputRing)+"  msgIdx:"+PipeReader.getMsgIdx(fes.inputRing));
						throw t;
					}
					
					
		        }
			
		}
		return;
	}

    private static void shutdownProcess(FASTEncodeStage fes) {
        PrimitiveWriter.assertAllFlushed(fes.writer);
        PrimitiveWriter.flush(fes.writer);
        
        while (!Pipe.roomToLowLevelWrite(fes.outputRing, Pipe.EOF_SIZE)) {	        				
        }	        			
        Pipe.publishEOF(fes.outputRing);
        
        Pipe.setReleaseBatchSize(fes.inputRing, 0);
        PipeReader.releaseReadLock(fes.inputRing);	        			
        
        fes.requestShutdown();
    }
	
	public static void flush(FASTEncodeStage fes) {	
		if (null!=fes) {
			PrimitiveWriter.flush(fes.writer);
		}
	}
	
	@Override
	public void run() {

		encodeUntilRingEmpty(this);
		//Does a lot of needless work if we flush here, flush(this);	

	}
	
	@Override
	public void shutdown() {
	    flush(this);
	}


	private static FASTDynamicWriter dynamicWriter(FASTEncodeStage fes) {
		if (null == fes.dynamicWriter) {
			
			FASTEncoder writerDispatch;
			boolean debug = false;
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
