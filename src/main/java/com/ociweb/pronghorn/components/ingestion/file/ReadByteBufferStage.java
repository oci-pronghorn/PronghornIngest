package com.ociweb.pronghorn.components.ingestion.file;

import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

/**
 * 
 * Loads unstructured binary file directly into bye array. TODO: may or may not keep. 
 *  
 * @author Nathan Tippy
 *
 */
public class ReadByteBufferStage implements Runnable {

	public final ByteBuffer activeByteBuffer;
	
	public final RingBuffer outputRing;
	
	public int recordStart = 0;
	
	public long recordCount = 0;
    private	long tailPosCache;
    private long targetValue;
    private final int stepSize;
    
    public final int publishCountDownInit;
    public int publishCountDown;
    
    public ReadByteBufferStage(ByteBuffer sourceByteBuffer, RingBuffer outputRing) {
    	this.activeByteBuffer=sourceByteBuffer;
    	
    	this.outputRing=outputRing;
    	
		if (RingBuffer.from(outputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		stepSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		publishCountDownInit = ((outputRing.mask+1)/stepSize)>>1;//count down to only half what the ring can hold
		publishCountDown = publishCountDownInit;
		
	    //NOTE: this block has constants that could be moved up and out
						
		int fill =1+outputRing.mask - stepSize;
		tailPosCache = tailPosition(outputRing);
		targetValue = tailPosCache-fill;
		if (outputRing.maxAvgVarLen<1) {
			throw new UnsupportedOperationException();
		}
		resetForNextByteBuffer(this);
    }
    
 
    protected static void resetForNextByteBuffer(ReadByteBufferStage lss) {
    	lss.recordStart = 0;    	
    }
    
    
	@Override
	public void run() {
		try{
			int position = parseSingleByteBuffer(this, activeByteBuffer);
			postProcessing(this, position);
		} catch (Exception e) {
			e.printStackTrace();
			RingBuffer.shutdown(outputRing);
		}
	}

	
	protected static void postProcessing(ReadByteBufferStage lss, int position) {
		//confirm end of file
		 if (position>lss.recordStart) {
			System.err.println("WARNING: last line of input did not end with LF or CR, possible corrupt or truncated file.  This line was NOT parsed.");
			//TODO: AA, note that passing this partial line messed up all other fields, so something is broken in the template loader when it is given bad data.
		 }
		 
        //before write make sure the tail is moved ahead so we have room to write
		lss.tailPosCache = spinBlockOnTail(lss.tailPosCache, lss.targetValue, lss.outputRing);
		lss.targetValue+=lss.stepSize;

		RingBuffer outputRing = lss.outputRing;
		//send end of file message
		RingBuffer.setValue(outputRing.structuredLayoutRingBuffer, outputRing.mask, RingBuffer.getWorkingHeadPositionObject(outputRing).value++, -1);
		RingBuffer.addNullByteArray(outputRing);
		RingBuffer.publishWrites(outputRing);
		
        //	    System.err.println("finished reading file "+recordCount);
	}

	protected static int parseSingleByteBuffer(ReadByteBufferStage lss, ByteBuffer sourceByteBuffer) {
		 
		 int maxBytes = lss.outputRing.maxAvgVarLen;
		 
		 int position = sourceByteBuffer.position();
		 int limit = sourceByteBuffer.limit();
		 while (position<limit) {
			 					   
			 			position += Math.min(limit-position, maxBytes);
			 									
						int len = position-lss.recordStart;
						//When we do smaller more frequent copies the performance drops dramatically. 5X slower.
						//The copy is an intrinsic and short copies are not as efficient
						
						sourceByteBuffer.position(lss.recordStart);
						RingBuffer outputRing = lss.outputRing;
						//before write make sure the tail is moved ahead so we have room to write
						lss.tailPosCache = spinBlockOnTail(lss.tailPosCache, lss.targetValue, outputRing);
						lss.targetValue+=lss.stepSize;
						
						RingBuffer.setValue(outputRing.structuredLayoutRingBuffer, outputRing.mask, RingBuffer.getWorkingHeadPositionObject(outputRing).value++, 0);
						assert(len>=0);
						int bytePos = RingBuffer.bytesWorkingHeadPosition(outputRing);
						
						RingBuffer.copyByteBuffer(sourceByteBuffer, len, outputRing);
						RingBuffer.addBytePosAndLen(outputRing, bytePos, len);
						
						lss.recordCount++;
						
						if (--lss.publishCountDown<=0) {
							RingBuffer.publishWrites(outputRing);
							lss.publishCountDown = lss.publishCountDownInit;
						}
						
						lss.recordStart = position;
													
								 
			 
		 }
		return position;
	}

}
