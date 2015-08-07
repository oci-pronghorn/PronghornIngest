package com.ociweb.pronghorn.components.ingestion.dynamic.stage;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.RecordFieldExtractor;
import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.TypeExtractor;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.MurmurHash;
import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.stream.AppendableUTF8Ring;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TemplateGeneratorStage extends PronghornStage {

	private final RingBuffer inputRing;
	private final RingBuffer outputRing;
	private final AppendableUTF8Ring output;	
	private static final Logger log = LoggerFactory.getLogger(TemplateGeneratorStage.class);
	private final RecordFieldExtractor extractNewSchema = new RecordFieldExtractor();   
	
	private int messageCount;
	/*
	 * Consumes Meta messages and produces new XML templates catalog upon receiving the flush message
	 */
	public TemplateGeneratorStage(GraphManager gm, RingBuffer inputRing, RingBuffer outputRing) {
		super(gm, inputRing, outputRing);
		this.inputRing = inputRing;
		this.outputRing = outputRing;
		this.output = new AppendableUTF8Ring(outputRing);
				
		if (RingBuffer.from(inputRing) !=  MetaMessageDefs.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the MetaFieldFROM catalog of messages for input.");
		}
		
		if (RingBuffer.from(outputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
				
	}
	
	//TODO: add SSN  and URL 
	
	@Override
	public void run() {
		try{
			collectUntilEndOfStream();
		} catch (Throwable e) {
			e.printStackTrace();
			RingBuffer.shutdown(inputRing);
			RingBuffer.shutdown(outputRing);			
		}
		
	}
	//TODO: AAA, should merge Long signed and unsinged fields
	//NOTE: important debug technique to stop old data from getting over written, TODO: how can we formalize this debug technique to make it easer.
	//RingWalker.setReleaseBatchSize(inputRing, 100000 );

	private void collectUntilEndOfStream() {
	
			while (RingReader.tryReadFragment(inputRing)) {
	        	assert(RingReader.isNewMessage(inputRing)) : "There are no multi fragment message found in the MetaFields";
	        	
	        	int msgLoc = RingReader.getMsgIdx(inputRing);
	        		        	
	        	String name =  MetaMessageDefs.FROM.fieldNameScript[msgLoc];
	        	int templateId = (int) MetaMessageDefs.FROM.fieldIdScript[msgLoc];	        	
	        	int type = 0x3F & (templateId>>1); //also contains name because we masked with 111111	        	
	        	int optionalFlag = (templateId & 1)<<RecordFieldExtractor.OPTIONAL_SHIFT;
	        		        	
	        	switch (type) {

	        		case 0: //UInt32	   
		        		{
		        			int uint = RingReader.readInt(inputRing,  MetaMessageDefs.UINT32_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema,uint);
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_UINT, uint, optionalFlag); 	
		        		}	
						break;
	        		case 64: //UInt32 Named	   
		        		{
		        			int uint = RingReader.readInt(inputRing, MetaMessageDefs.NAMEDUINT32_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema,uint);
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_UINT, uint, optionalFlag, inputRing, MetaMessageDefs.NAMEDUINT32_NAME_LOC); 	
		        		}	
						break;
						
	        		case 1: //Int32	      
		        		{
							int sint = RingReader.readInt(inputRing, MetaMessageDefs.INT32_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema,sint);
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_SINT, sint, optionalFlag);
		        		}
		        		break;
	        		case 65: //Int32 Named      
		        		{
							int sint = RingReader.readInt(inputRing, MetaMessageDefs.NAMEDINT32_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema,sint);
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_SINT, sint, optionalFlag, inputRing, MetaMessageDefs.NAMEDINT32_NAME_LOC); 	
		        		}
		        		break;
		        		
	        		case 2: //UInt64
		        		{
							long ulong = RingReader.readLong(inputRing, MetaMessageDefs.UINT64_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema, (int)ulong); //this is OK, its only used to decide on compression style
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_ULONG, ulong, optionalFlag);	
		        		}
		        		break;
	        		case 66: //UInt64 Named
		        		{
							long ulong = RingReader.readLong(inputRing, MetaMessageDefs.NAMEDUINT64_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema, (int)ulong); //this is OK, its only used to decide on compression style
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_ULONG, ulong, optionalFlag, inputRing, MetaMessageDefs.NAMEDUINT64_NAME_LOC); 	
		        		}
		        		break;
	        				        		
	        		case 3: //Int64
		        		{
							long slong = RingReader.readLong(inputRing, MetaMessageDefs.INT64_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema,(int)slong); //this is OK, its only used to decide on compression style
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_SLONG, slong, optionalFlag);
		        		}
	        			break;
	        		case 67: //Int64 Named
		        		{
							long slong = RingReader.readLong(inputRing, MetaMessageDefs.NAMEDINT64_VALUE_LOC);
							RecordFieldExtractor.activeFieldHash(extractNewSchema,(int)slong); //this is OK, its only used to decide on compression style
							RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_SLONG, slong, optionalFlag, inputRing, MetaMessageDefs.NAMEDINT64_NAME_LOC); 
		        		}
		        		break;      			
	        			
	        		case 4: //ASCII
		        		{
		        			//TODO: C, this has room for improvement but it was quick to write
		        			int readBytesLength = RingReader.readBytesLength(inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
		        			int readBytesPos    = RingReader.readBytesPosition(inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
		        			byte[] backing      = RingReader.readBytesBackingArray(inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
		        			
		        			RecordFieldExtractor.activeFieldHash(extractNewSchema, MurmurHash.hash32(backing, readBytesPos, readBytesLength, inputRing.byteMask, extractNewSchema.someSeed));
		        			RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_ASCII, readBytesLength, optionalFlag);        			
		        		}
		        		break;
	        		case 68: //ASCII Named
		        		{
		        			//TODO: C, this has room for improvement but it was quick to write
		        			int readBytesLength = RingReader.readBytesLength(inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
		        			int readBytesPos    = RingReader.readBytesPosition(inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
		        			byte[] backing      = RingReader.readBytesBackingArray(inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
		        			
		        			RecordFieldExtractor.activeFieldHash(extractNewSchema, MurmurHash.hash32(backing, readBytesPos, readBytesLength, inputRing.byteMask, extractNewSchema.someSeed));
		        			RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_ASCII, readBytesLength, optionalFlag, inputRing, MetaMessageDefs.NAMEDASCII_NAME_LOC); 
		        		}
	        			break;       			
	        			
	        		case 6: //Decimal
		        		{
		        			//TODO: B, this seems short sighted because the exponent may be important.
		        			long readDecimalMantissa = RingReader.readDecimalMantissa(inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
		        			RecordFieldExtractor.activeFieldHash(extractNewSchema, (int)readDecimalMantissa); //this is OK, its only used to decide on compression style
						    RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_DECIMAL, readDecimalMantissa, optionalFlag);
		        		}
	        	        break;
	        		case 70: //Decimal Named
		        		{
		        			//TODO: B, this seems short sighted because the exponent may be important.
		        			long readDecimalMantissa = RingReader.readDecimalMantissa(inputRing, MetaMessageDefs.NAMEDDECIMAL_VALUE_LOC);
		        			RecordFieldExtractor.activeFieldHash(extractNewSchema, (int)readDecimalMantissa); //this is OK, its only used to decide on compression style
						    RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_DECIMAL, readDecimalMantissa, optionalFlag, inputRing, MetaMessageDefs.NAMEDDECIMAL_NAME_LOC); 
		        		}
	        	        break;     	        
	        	        	        	        
	        	        
	        		case 16: //beginMessage
	        			RecordFieldExtractor.resetToRecordStart(extractNewSchema);        			
	        			break;
	        		case 80: //beginMessage Named
	        			RecordFieldExtractor.resetToRecordStart(extractNewSchema);        			
	        			break;
	        		case 17: //endMessage        			
	        			RecordFieldExtractor.appendNewRecord(extractNewSchema); 
	        			messageCount++;
	        			break;
		        			        	

	        		case 18: //Null
	        			RecordFieldExtractor.activeFieldHash(extractNewSchema, 0);
	        			RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_NULL, 0, RecordFieldExtractor.OPTIONAL_FLAG);
	        			break;
	        		case 82: //Null Named
	        			RecordFieldExtractor.activeFieldHash(extractNewSchema, 0);
	        			RecordFieldExtractor.appendNewField(extractNewSchema, TypeExtractor.TYPE_NULL, 0, RecordFieldExtractor.OPTIONAL_FLAG, inputRing, MetaMessageDefs.NAMED_NULL_NAME_LOC);
	        			break;	
	        			
	        			
	        		case 31: //flush

	        			log.info("flush the catalog");
	        			System.err.println("begin cat gen*******************************************8");
	        			long start = System.currentTimeMillis();
	        			
	        			extractNewSchema.mergeNumerics(0);//TODO: why are two passes needed here?
	        			extractNewSchema.mergeOptionalNulls(0);
	        		//	extractNewSchema.mergeNumerics(0);
	        	//		extractNewSchema.mergeOptionalNulls(0);
	        			extractNewSchema.printRecursiveReport(0, "  ");
	        			
	        			boolean addCatBytesMessage = false;
	        			try {
	        				
	        				//this streams out the XML catalog to the output ring buffer
	        				RecordFieldExtractor.buildCatalog(extractNewSchema, addCatBytesMessage, output);
	        				output.flush();
	        				
	        				long duration = System.currentTimeMillis()-start;
	        				System.err.println("cat gen duration:"+duration);//~25% of the time is here building the catalog.
	        			} catch (IOException e) {
	        				throw new RuntimeException(e);
	        			}
	        			//continues to run to consume more fields and update the catalog if needed
	        			if (true) {
	        				requestShutdown();
	        				return; //needed to complete test. TODO: A, revisit how this should exit and if it should exit.
	        			}
	        			break;
	        	    default:
	        	    	log.error("Missing case for:"+templateId+" "+name);
	        	
	        	}
	        	
	        	log.trace("Name:{} {} {}",name,msgLoc,templateId);
	        	
		 } 

	}

	public long getMessages() {
		return extractNewSchema.totalRecords;
	}

}
