package com.ociweb.pronghorn.components.ingestion.dynamic.stage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.RecordFieldExtractor;
import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.TypeExtractor;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.MurmurHash;
import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MessageGeneratorStage extends PronghornStage {

	private static final Logger log = LoggerFactory.getLogger(MessageGeneratorStage.class);
			
	private final Pipe inputRing;
	private final Pipe outputRing;	
	private final RecordFieldExtractor extractNewSchema = new RecordFieldExtractor();   
	
	private int messageTemplateIdHash;
	private boolean startingMessage;
	private final int maxFragmentSize;
	
	
	boolean isInsideMessage = false;
	long messagesCount;
	
	/**
	 * Converts meta messages into business messages.
	 * 
	 * @param gm
	 * @param inputRing
	 * @param outputRing
	 */
	public MessageGeneratorStage(GraphManager gm, Pipe inputRing, Pipe outputRing) {
		super(gm,inputRing,outputRing);
		this.inputRing = inputRing;
		this.outputRing = outputRing;
		
		if (Pipe.from(inputRing) != MetaMessageDefs.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the MetaFieldFROM catalog of messages for input.");
		}

		//pickup whatever schema that the output ring buffer was initialized with
		FieldReferenceOffsetManager from = Pipe.from(outputRing);
		extractNewSchema.loadFROM(from);
		
		maxFragmentSize = FieldReferenceOffsetManager.maxFragmentSize(from);
		
		startingMessage = true;  
		
		
	}
	
	long offestForMsgIdx = -1;
	StringBuilder builder = new StringBuilder();

	private long keepOff;

	private long keepHead;
	
	@Override
	public void run() {

				while (PipeReader.tryReadFragment(inputRing)) {			
	
					assert(PipeReader.isNewMessage(inputRing)) : "There are no multi fragment message found in the MetaFields";
		        	
		        	int msgLoc = PipeReader.getMsgIdx(inputRing);
		        	
		        	String name = MetaMessageDefs.FROM.fieldNameScript[msgLoc];
		        	
		        	//The templateIds for the Meta templates contains information on the type of meta message sent.
		        	//  -  the low bit indicates if the field this message represents is optional
		        	//  -  the middle bits indicate the type and the high bit indicates it has a name.
		        	long metaMsgTmplId = MetaMessageDefs.FROM.fieldIdScript[msgLoc];	 
		        	
		        	int tSwitch = 0x3F & (int)(metaMsgTmplId>>1); //also contains name because we masked with 111111	
		        	int optionalFlag = (int) ((metaMsgTmplId & 1)<<RecordFieldExtractor.OPTIONAL_SHIFT);
		        	
		        	switch (tSwitch) {
	
		        		case 0: //UInt32	   
			        		{
			        			int value = PipeReader.readInt(inputRing, MetaMessageDefs.UINT32_VALUE_LOC);		        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema,value);
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_UINT, optionalFlag); 	
			        			writeInt(outputRing,type,value);
		
			        		}	
							break;
		        		case 64: //UInt32 Named	   
			        		{
			        			int value = PipeReader.readInt(inputRing, MetaMessageDefs.NAMEDUINT32_VALUE_LOC);
			        			builder.setLength(0);
								RecordFieldExtractor.activeFieldHash(extractNewSchema,value);
								
								int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_UINT, optionalFlag,inputRing, MetaMessageDefs.NAMEDUINT32_NAME_LOC);		
								writeInt(outputRing, type,value);
			
			        		}	
							break;
							
		        		case 1: //Int32	      
			        		{
			        			int value = PipeReader.readInt(inputRing, MetaMessageDefs.INT32_VALUE_LOC);		        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema,value);
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_SINT, optionalFlag); 	
			        			writeInt(outputRing,type,value);
	
			        		}
			        		break;
		        		case 65: //Int32 Named      
			        		{
			        			int value = PipeReader.readInt(inputRing, MetaMessageDefs.NAMEDINT32_VALUE_LOC);		        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema,value);
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_SINT, optionalFlag,inputRing, MetaMessageDefs.NAMEDINT32_NAME_LOC); 	
			        			writeInt(outputRing,type,value);
		
			        		}
			        		break;
			        		
		        		case 2: //UInt64
			        		{
			        			long value = PipeReader.readLong(inputRing, MetaMessageDefs.UINT64_VALUE_LOC);		        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema,(int)value);
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_ULONG, optionalFlag); 	
			        			writeLong(outputRing,type,value);
			        				
			        		}
			        		break;
		        		case 66: //UInt64 Named
			        		{
			        			long value = PipeReader.readLong(inputRing, MetaMessageDefs.NAMEDUINT64_VALUE_LOC);		        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema,(int)value);
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_ULONG, optionalFlag, inputRing, MetaMessageDefs.NAMEDUINT64_NAME_LOC); 	
			        			writeLong(outputRing,type,value);
	
			        		}
			        		break;
		        				        		
		        		case 3: //Int64
			        		{
			        			long value = PipeReader.readLong(inputRing, MetaMessageDefs.INT64_VALUE_LOC);		        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema,(int)value);
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_ULONG, optionalFlag); 	
			        			writeLong(outputRing,type,value);
			        				
			        		}
		        			break;
		        		case 67: //Int64 Named
			        		{
			        			long value = PipeReader.readLong(inputRing, MetaMessageDefs.NAMEDINT64_VALUE_LOC);		        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema,(int)value);
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_ULONG, optionalFlag, inputRing, MetaMessageDefs.NAMEDINT64_NAME_LOC); 	
			        			writeLong(outputRing,type,value);
			         
			        		}
			        		break;      			
		        			
		        		case 4: //ASCII
			        		{
			        			
			        			//TODO: C, this has room for improvement but it was quick to write
			        			int readBytesLength = PipeReader.readBytesLength(inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
			        			int readBytesPos    = PipeReader.readBytesPosition(inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
			        			byte[] backing      = PipeReader.readBytesBackingArray(inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
			        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema, MurmurHash.hash32(backing, readBytesPos, readBytesLength, inputRing.byteMask, extractNewSchema.someSeed));
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_ASCII, optionalFlag); 	
			        			writeBytes(outputRing, type, backing, readBytesPos, readBytesLength, inputRing.byteMask);
			        			     			
			        			//keep this in case we need it for the message template identification
			        			if (1==extractNewSchema.fieldCount) {
			        				int j = RecordFieldExtractor.activeFieldHash(extractNewSchema);
			        				//System.err.println("new hash "+j+" for "+ new String(backing,readBytesPos,readBytesLength));
			        				messageTemplateIdHash = extractNewSchema.bc.mask + (j & extractNewSchema.bc.mask);
			        			}   
			        		}
			        		break;
		        		case 68: //ASCII Named
			        		{
			                	
			        			//TODO: C, this has room for improvement but it was quick to write
			        			int readBytesLength = PipeReader.readBytesLength(inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
			        			int readBytesPos    = PipeReader.readBytesPosition(inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
			        			byte[] backing      = PipeReader.readBytesBackingArray(inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
			        			
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema, MurmurHash.hash32(backing, readBytesPos, readBytesLength, inputRing.byteMask, extractNewSchema.someSeed));
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_ASCII, optionalFlag, inputRing, MetaMessageDefs.NAMEDASCII_NAME_LOC); 	
			        			writeBytes(outputRing, type, backing, readBytesPos, readBytesLength, inputRing.byteMask);
			        			
			        			//keep this in case we need it for the message template identification
			        			if (1==extractNewSchema.fieldCount) {
			        				int j = RecordFieldExtractor.activeFieldHash(extractNewSchema);
			        				messageTemplateIdHash = extractNewSchema.bc.mask + (j & extractNewSchema.bc.mask);
			        			} 
			        		}
		        			break;       			
		        			
		        		case 6: //Decimal
			        		{
			        			int readDecimalExponent = PipeReader.readDecimalExponent(inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
			        			long readDecimalMantissa = PipeReader.readDecimalMantissa(inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema, (int)readDecimalMantissa); //this is OK, its only used to decide on compression style
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_DECIMAL, optionalFlag); 	
			        			writeDecimal(outputRing, type, readDecimalExponent, readDecimalMantissa);
			        			
			        		}
		        	        break;
		        		case 70: //Decimal Named
			        		{
			        			int readDecimalExponent = PipeReader.readDecimalExponent(inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
			        			long readDecimalMantissa = PipeReader.readDecimalMantissa(inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema, (int)readDecimalMantissa); //this is OK, its only used to decide on compression style
			        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_DECIMAL, optionalFlag, inputRing, MetaMessageDefs.NAMEDDECIMAL_NAME_LOC); 	
			        			writeDecimal(outputRing, type, readDecimalExponent, readDecimalMantissa);
	
			        		}
		        	        break;     	       
		        	        	        	        
		        	        
		        		case 16: //beginMessage
			        		{
			        			
			        			assert(!isInsideMessage);
			        			isInsideMessage = true;
			        			
			        			RecordFieldExtractor.resetToRecordStart(extractNewSchema);   
			        			
			        			//no idea how much space we will need for this message so we block for the largest known fragment size
			        			while (!Pipe.roomToLowLevelWrite(outputRing, maxFragmentSize)) {
			        				
			        			}
			        			
						    	if (startingMessage) {
						    		 //this call only needed because we will be writing the message without knowing the message id until we are done
						    		 Pipe.markBytesWriteBase(outputRing);
						    		 offestForMsgIdx = Pipe.getWorkingHeadPositionObject(outputRing).value++; 
						    		 
						    		 keepOff = offestForMsgIdx;
						    		 keepHead = Pipe.headPosition(outputRing);
						    		 
						    		 assert(offestForMsgIdx >= Pipe.headPosition(outputRing)) : "head position has moved out in front of the message that has not been defined. PublishedHead:"+Pipe.headPosition(outputRing)+" msgIdxOffset:"+offestForMsgIdx;

						    		 
						    		 startingMessage = false;
						    		 						    		 
						    	} else {
						    		log.error("NOT starting message");
						    	}
						    		
			        		}
		        			break;
		        		case 80: //beginMessage Named
			        		{
			        			
			        			assert(!isInsideMessage);
			        			isInsideMessage = true;
			        			
			        			RecordFieldExtractor.resetToRecordStart(extractNewSchema);   
			        			
			        			//no idea how much space we will need for this message so we block for the largest known fragment size
			        			while (!Pipe.roomToLowLevelWrite(outputRing, maxFragmentSize)) {
			        				
			        			}
			        			
						    	if (startingMessage) {
						    		 //this call only needed because we will be writing the message without knowing the message id until we are done
						    		 Pipe.markBytesWriteBase(outputRing);
						    		 
						    		 offestForMsgIdx = Pipe.getWorkingHeadPositionObject(outputRing).value++; 
							    	
						    		 keepOff = offestForMsgIdx;
						    		 keepHead = Pipe.headPosition(outputRing);
						    		 
						    		 assert(offestForMsgIdx >= Pipe.headPosition(outputRing)) : "head position has moved out in front of the message that has not been defined. PublishedHead:"+Pipe.headPosition(outputRing)+" msgIdxOffset:"+offestForMsgIdx;

						    		 startingMessage = false;
						    		 						    		 
						    	} else {
						    		log.error("NOT starting message");
						    	}
			        		}
		        			break;
		        		case 17: //endMessage        	
		    	        			
		        			assert(isInsideMessage);
		        			isInsideMessage = false;
		        			messagesCount++;
		        			
		        			//will remain -1 if the message id is not used because there is only one template.
		        			if (offestForMsgIdx>=0) {	
		        				assert(keepHead == Pipe.headPosition(outputRing));
		        				assert(keepOff == offestForMsgIdx);
		        				
		        				assert(offestForMsgIdx >= Pipe.headPosition(outputRing)) : "head position has moved out in front of the message that has not been defined. PublishedHead:"+Pipe.headPosition(outputRing)+" msgIdxOffset:"+offestForMsgIdx;

		        				//log.warn("new message "+offestForMsgIdx);
		        				
		        				int msgIdx = extractNewSchema.messageIdx(messageTemplateIdHash);
		        				
		        				//This validation is very important, because all down stream consumers will assume it to be true.
		        				assert(TypeMask.Group == TokenBuilder.extractType(Pipe.from(outputRing).tokens[msgIdx])) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(Pipe.from(outputRing).tokens[msgIdx]);
		        				assert((OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(Pipe.from(outputRing).tokens[msgIdx])) == 0) : "Templated message must start with group open and this starts with "+TokenBuilder.tokenToString(Pipe.from(outputRing).tokens[msgIdx]);
		 
		        				Pipe.setValue(outputRing.slabRing, outputRing.mask, offestForMsgIdx, msgIdx);
		        				
		        				//only need to set this because we waited until now to know what the message ID was
		        				Pipe.confirmLowLevelWrite(outputRing, Pipe.from(outputRing).fragDataSize[msgIdx]);	
		        						
		        				
		        				//log.warn("finished publish of message {} starting at {} ending at {} msgCount:"+messagesCount,msgIdx+" size:"+RingBuffer.from(outputRing).fragDataSize[msgIdx]+" idx "+msgIdx, offestForMsgIdx, outputRing.workingHeadPos.value);
		        				
		        				offestForMsgIdx = -1;
		        			} else {
					    		log.error("SKIPPED SETTING MSG ID");
					    	}
		        			
		        			
		        	        Pipe.publishWrites(outputRing);
		        	        //log.error("pub "+RingBuffer.headPosition(outputRing)+"  "+outputRing.workingHeadPos.value+"  ");

		        	        
		        	        startingMessage = true;	   
		        	        extractNewSchema.totalRecords++;
		        	        
		        			break;
			        			        	
	
		        		case 18: //Null
			        		{
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema, 0);	        			
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_NULL, RecordFieldExtractor.OPTIONAL_FLAG);
			        			writeNull(outputRing, type);
			        		}
		        			break;
		        		case 82: //Null Named
			        		{
			        			RecordFieldExtractor.activeFieldHash(extractNewSchema, 0);
			        			int type = RecordFieldExtractor.moveNextField(extractNewSchema, TypeExtractor.TYPE_NULL, RecordFieldExtractor.OPTIONAL_FLAG, inputRing, MetaMessageDefs.NAMED_NULL_NAME_LOC);
			        			writeNull(outputRing, type);
			        		}
		        			break;	
		        			
		        			
		        		case 31: //flush
		        		    Pipe.setReleaseBatchSize(inputRing, 0);
		        			PipeReader.releaseReadLock(inputRing);
		        			
		        			
//		        			//double check that all messages are closed
//		        			assert(offestForMsgIdx == -1);
		        			
		        			//no idea how much space we will need for this message so we block for the largest known fragment size
		        			while (!Pipe.roomToLowLevelWrite(outputRing, Pipe.EOF_SIZE)) {
		        				
		        			}
		        			
		        			Pipe.publishEOF(outputRing);
		        			Pipe.confirmLowLevelWrite(outputRing, Pipe.EOF_SIZE);		        					
		        			
		//        			if (true) {
		        				requestShutdown();
		        				return; //needed to complete test. TODO: A, revisit how this should exit and if it should exit.
		//        			}
		//        			break;
		        	    default:
		        	    	log.error("Missing case for:"+metaMsgTmplId+" "+name);
		        	
		        	}
		        	
		        	assert(!isInsideMessage || keepHead==Pipe.headPosition(outputRing)) : "changed head in field  :"+tSwitch+"  "+keepHead+" vs "+Pipe.headPosition(outputRing);
		        	log.trace("Name:{} {} {}",name,msgLoc,metaMsgTmplId);
		        	
			 }
		
			
			
		}


	//writes null
	private void writeNull(Pipe outputRing, int type) {
		//System.err.println("write NULL:"+type);
		switch (type) {
//			case TypeExtractor.TYPE_SINT:
//			case TypeExtractor.TYPE_UINT:
//				RingWriter.writeInt(outputRing, FieldReferenceOffsetManager.getAbsent32Value(RingBuffer.from(outputRing)));
//				break;
//			case TypeExtractor.TYPE_SLONG:
//			case TypeExtractor.TYPE_ULONG:
//				RingWriter.writeLong(outputRing, FieldReferenceOffsetManager.getAbsent64Value(RingBuffer.from(outputRing)));
//				break;
			case TypeExtractor.TYPE_ASCII:
			case TypeExtractor.TYPE_BYTES:
			case TypeExtractor.TYPE_NULL:	
				Pipe.addNullByteArray(outputRing);
				break;
//			case TypeExtractor.TYPE_DECIMAL:
//				RingWriter.writeDecimal(outputRing, 
//						                FieldReferenceOffsetManager.getAbsent32Value(RingBuffer.from(outputRing)), 
//						                FieldReferenceOffsetManager.getAbsent64Value(RingBuffer.from(outputRing)));
//				break;
			default:
					throw new UnsupportedOperationException("TODO still need to implement all the types, missing "+type);
								
		}
	}

	private void writeDecimal(Pipe outputRing, int type, int readDecimalExponent, long readDecimalMantissa) {
		//System.err.println("write bytes decimal:"+readDecimalMantissa);
		switch (type) {
			case TypeExtractor.TYPE_DECIMAL:
			Pipe.addValues(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing), readDecimalExponent, readDecimalMantissa);
			break;
			
			case TypeExtractor.TYPE_SLONG:
			case TypeExtractor.TYPE_ULONG:
			Pipe.addLongValue(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing), (long) Math.rint(readDecimalMantissa * PipeReader.powfi[64 + readDecimalExponent]));
			break;
			case TypeExtractor.TYPE_ASCII:
								
				throw new UnsupportedOperationException("TODO still need to implement all the types, missing write Decimal as ASCII but why do I want this?");
				
			default:
				throw new UnsupportedOperationException("TODO still need to implement all the types, missing "+type);
					
		
		}	
	}

	private void writeBytes(Pipe outputRing, int type, byte[] backing, int readBytesPos, int readBytesLength, int byteMask) {
		
		
		
		assert(readBytesLength>=0);
		switch (type) {
			case TypeExtractor.TYPE_ASCII:
			case TypeExtractor.TYPE_BYTES:
				assert (readBytesLength<backing.length);
				
			//	System.err.println("write bytes len:"+readBytesLength+" String:"+new String(backing,readBytesPos,readBytesLength));
				
		//		RingReader.copyBytes(outputRing, outputRing, fieldId)
				
				int length1 = 1+byteMask-(byteMask&readBytesPos);
				if (length1>=readBytesLength) {
					Pipe.addByteArray(backing, byteMask&readBytesPos, readBytesLength, outputRing);				
				} else {
					//System.err.println("write rollover for ascii");
					
					Pipe.validateVarLength(outputRing, readBytesLength);
					//write from two places into new ring buffer but it must be only 1 field
					int pos = Pipe.bytesWorkingHeadPosition(outputRing);
					Pipe.copyBytesFromToRing(backing, byteMask&readBytesPos, Integer.MAX_VALUE, outputRing.blobRing, pos, outputRing.byteMask, length1);
					Pipe.copyBytesFromToRing(backing, 0, Integer.MAX_VALUE, outputRing.blobRing, pos+length1, outputRing.byteMask, readBytesLength-length1);
					Pipe.addBytePosAndLen(outputRing, pos, readBytesLength);
					
					Pipe.setBytesWorkingHead(outputRing,pos + readBytesLength);
				}
				break;
			case TypeExtractor.TYPE_DECIMAL:
				
				
				//RingBuffer.addDecimalAsASCII(0,0,outputRing);
				
				//need to convert these bytes into a decimal value.
				throw new UnsupportedOperationException("TODO still need to implement all the types, missing DECIMAL from bytes for "+new String(backing,readBytesPos, readBytesLength));
				
			//	break;
			default:
					throw new UnsupportedOperationException("TODO still need to implement all the types, missing "+type);
								
		}
	}
	
	private static void writeInt(Pipe outputRing, int type, int value) {
		//System.err.println("write int:"+value);
		switch (type) {
			case TypeExtractor.TYPE_SINT:
			case TypeExtractor.TYPE_UINT:
			Pipe.setValue(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing).value++, value);
			break;
			case TypeExtractor.TYPE_SLONG:
			case TypeExtractor.TYPE_ULONG:
			Pipe.addLongValue(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing), (long) value);
			break;
			case TypeExtractor.TYPE_ASCII:
			case TypeExtractor.TYPE_BYTES:	
			Pipe.addIntAsASCII(outputRing, value);
			break;
			case TypeExtractor.TYPE_DECIMAL:
			Pipe.addValues(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing), 0, (long) value);
			break;
			default:
					throw new UnsupportedOperationException("TODO still need to implement all the types, missing "+type);
					
		
		}		
	}

	private static void writeLong(Pipe outputRing, int type, long value) {
		//System.err.println("write long:"+value);
		switch (type) {
			case TypeExtractor.TYPE_SINT:
			case TypeExtractor.TYPE_UINT:
			Pipe.setValue(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing).value++, (int)value);
			break;
			case TypeExtractor.TYPE_SLONG:
			case TypeExtractor.TYPE_ULONG:
			Pipe.addLongValue(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing), value);
			break;
			case TypeExtractor.TYPE_ASCII:
			case TypeExtractor.TYPE_BYTES:	
			Pipe.addLongAsASCII(outputRing, value);
			break;			
			case TypeExtractor.TYPE_DECIMAL:
			Pipe.addValues(outputRing.slabRing, outputRing.mask, Pipe.getWorkingHeadPositionObject(outputRing), 0, value);
			break;
			default:
					throw new UnsupportedOperationException("TODO still need to implement all the types, missing "+type);
						
		
		}		
	}

	public long getMessages() {
		return extractNewSchema.totalRecords;
	}

}
