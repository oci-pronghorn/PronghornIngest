package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.BloomCounter;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.util.hash.LongHashTable;

@Deprecated
public class StreamingVisitor implements ExtractionVisitor {

    public static final int CATALOG_TEMPLATE_ID = 0;
    
    public RecordFieldExtractor messageTypes;   
    
    byte[] catBytes;
    
    long beforeDotValue;
    int beforeDotValueChars;
    long accumValue;
    int accumValueChars;
    long accumSign;
    boolean aftetDot;
    long offestForTemplateId = -1;

    boolean startingMessage;
    
    
    

    String lastClosedLine = "";
    
	boolean isAlive = true;

    
    //chars are written to  ring buffer.
    
    int bytePosActive;
    int bytePosStartField;
    
    private RingBuffer ringBuffer;   
    private int fill;
	private long tailPosCache;
    
    int messageTemplateIdHash;

	private LongHashTable templateIdStart;
	private TypeExtractor typeExtractor;
        
    
    public StreamingVisitor(RecordFieldExtractor messageTypes, LongHashTable templateIdStart, RingBuffer ringBuffer, TypeExtractor typeExtractor) {
    	 	    	
    	//initial ring buffer needed only for the first catalog then we move on from this one.
    	byte[] catBytes = messageTypes.getCatBytes();
    	if (null==catBytes) {
    		catBytes = messageTypes.memoizeCatBytes(); //must use a ring buffer from catalog or it will not be initilzied for use.
    	}
    	this.catBytes = catBytes;
    	this.templateIdStart = templateIdStart;
		this.ringBuffer = ringBuffer;
		
		this.fill =  1 + ringBuffer.mask - 4;
		this.tailPosCache = tailPosition(ringBuffer);
		
		
        this.messageTypes = messageTypes;   
        this.typeExtractor = typeExtractor;
        
        TypeExtractor.resetFieldSum(typeExtractor);
		RecordFieldExtractor.resetToRecordStart(messageTypes);
        
        aftetDot = false;
        beforeDotValue = 0;
        beforeDotValueChars = 0;
        
        accumSign = 1;
        
        accumValue = 0;
        accumValueChars = 0;
        
        startingMessage = true;		
        
    }
    
    
    @Override
    public void appendContent(ByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
                
    	
    	
        //discovering the field types using the same way the previous visitor did it
        TypeExtractor.appendContent(typeExtractor, mappedBuffer, pos, limit);
        RecordFieldExtractor.activeFieldHash(messageTypes, BloomCounter.hash(mappedBuffer,pos,limit-pos,messageTypes.bc));
                  
      //  StringBuilder temp = new StringBuilder();
        
        //keep bytes here in case we need it, will only be known after we are done
        int p = pos;
        while (p<limit) {
            byte b = mappedBuffer.get(p);
            //May be ASCII or Bytes, we trust the FAST encoder to stip out any high bits if its the wrong encoding.
            ringBuffer.unstructuredLayoutRingBuffer[ringBuffer.byteMask&bytePosActive++] = (byte)b;
          //  temp.append((char)b);
                        
            if ('.' == b) {
                aftetDot = true;
                beforeDotValue = accumValue;
                beforeDotValueChars = accumValueChars;
                accumValue = 0;
                accumValueChars = 0;
            } else {
               if ('+' != b) {
                   if ('-' == b) { 
                       accumSign = -1;
                   } else {
                       int v = (b-'0');
                       accumValue = (10*accumValue) + v;    
                       accumValueChars++;
                   }                    
               }
            }
            p++;
        } 
        
      //  System.err.println(messageTypes.fieldCount+"   parse:"+temp);
    }

    @Override
    public void closeRecord(ByteBuffer mappedBuffer, int startPos) {
    	
    	lastClosedLine = startPos<mappedBuffer.position() ? new String(messageTypes.bytesForLine(mappedBuffer, startPos)) : "";
    	
    	//will remain -1 if the message id is not used because there is only one template.
    	if (offestForTemplateId>=0) {
	    	try {
	    		int msgIdx = messageTypes.messageIdx(messageTemplateIdHash);
	    		
	    		RingBuffer.setValue(ringBuffer.structuredLayoutRingBuffer, ringBuffer.mask, offestForTemplateId, msgIdx);
	    	} catch (Throwable t) {
	    		System.err.println(lastClosedLine);
	    		t.printStackTrace();
	    		System.exit(-1);
	    	}
	    	
	    	offestForTemplateId = -1;
    	}
        TypeExtractor.resetFieldSum(typeExtractor);
		RecordFieldExtractor.resetToRecordStart(messageTypes);
                
        //move the pointer up to the next record
		RingBuffer.setBytesWorkingHead(ringBuffer, bytePosStartField = bytePosActive);

        RingBuffer.publishWrites(ringBuffer);
        startingMessage = true;  
        
        messageTypes.totalRecords++;
        
    }

    
    @Override
    public boolean closeField(ByteBuffer mappedBuffer, int startPos) {
    	
    	tailPosCache = RingBuffer.spinBlockOnTail(tailPosCache, headPosition(ringBuffer)-fill, ringBuffer);
    
    	
    	if (startingMessage) {
    		 offestForTemplateId = RingBuffer.getWorkingHeadPositionObject(ringBuffer).value++; 
    	}
    	
    	int fieldType = TypeExtractor.extractType(typeExtractor);        
		TypeExtractor.resetFieldSum(typeExtractor); 
		
		fieldType = RecordFieldExtractor.moveNextField(messageTypes, fieldType, 0);//TODO: unsure if we need to mark the optional.  

    	
        switch (fieldType) {
            case TypeExtractor.TYPE_NULL:
            	
            	System.err.println("last closed line: "+lastClosedLine);
            	String example = new String(messageTypes.bytesForLine(mappedBuffer, startPos));
            	
                //TODO: what optional types are available? what if there are two then follow the order.
                new Exception("Require optional field but unable to find one in field "+messageTypes.fieldCount+" records "+messageTypes.totalRecords+" example "+example).printStackTrace();
                
                break;
            case TypeExtractor.TYPE_UINT:                
            case TypeExtractor.TYPE_SINT:
			RingBuffer.setValue(ringBuffer.structuredLayoutRingBuffer, ringBuffer.mask, RingBuffer.getWorkingHeadPositionObject(ringBuffer).value++, (int)(accumValue*accumSign));  
                break;   
            case TypeExtractor.TYPE_ULONG:
            case TypeExtractor.TYPE_SLONG:
			RingBuffer.addLongValue(ringBuffer.structuredLayoutRingBuffer, ringBuffer.mask, RingBuffer.getWorkingHeadPositionObject(ringBuffer), accumValue*accumSign);  
                break;    
            case TypeExtractor.TYPE_ASCII:
            	
            	if (1==messageTypes.fieldCount) {
            		int j = RecordFieldExtractor.activeFieldHash(messageTypes);
            		messageTemplateIdHash = messageTypes.bc.mask + (RecordFieldExtractor.activeFieldHash(messageTypes) & messageTypes.bc.mask);
            	}
			int length = bytePosActive-bytePosStartField;            	
            	      
            	RingBuffer.validateVarLength(ringBuffer, length);
			RingBuffer.addBytePosAndLen(ringBuffer, bytePosStartField, length);
			
			RingBuffer.setBytesWorkingHead(ringBuffer, bytePosStartField + length);
            	
                break;
            case TypeExtractor.TYPE_BYTES:
			int length1 = bytePosActive-bytePosStartField; 
            	RingBuffer.validateVarLength(ringBuffer, length1);
			RingBuffer.addBytePosAndLen(ringBuffer,  bytePosStartField, length1);
			RingBuffer.setBytesWorkingHead(ringBuffer, bytePosStartField + length1);
                break;
            case TypeExtractor.TYPE_DECIMAL:
            	
                final int exponent = accumValueChars;                
                long totalValue = (beforeDotValue*TypeExtractor.POW_10[accumValueChars])+accumValue;
                
		    	long mantissa = totalValue*accumSign;

			RingBuffer.addValues(ringBuffer.structuredLayoutRingBuffer, ringBuffer.mask, RingBuffer.getWorkingHeadPositionObject(ringBuffer), exponent, mantissa);  
                break;
            
            default:
                throw new UnsupportedOperationException("Field was "+fieldType);
        }       
        
        //long end = ringBuffer.workingHeadPos.value;      
        //System.err.println("field finish at "+end+" templateOffset "+offestForTemplateId);
                
        
        //closing field so keep this new active position as the potential start for the next field
        bytePosStartField = bytePosActive;
        // ** write as we go close out the field

        resetFieldStateCounters();
                
        startingMessage = false;
        
        
        return true;//this was successful so continue
        
    }


	private void resetFieldStateCounters() {
		aftetDot = false;
        beforeDotValue = 0;
        accumSign = 1;
        accumValue = 0;
        beforeDotValueChars = 0;
        accumValueChars = 0;
	}


    
    @Override
    public void closeFrame() {   
    		
    }

    @Override
    public void openFrame() {
    	boolean debug = false;
    	if (debug) {
    		System.err.println("****************** open frame:");
    	}
    	
    	resetFieldStateCounters();
    	TypeExtractor.resetFieldSum(typeExtractor);
		RecordFieldExtractor.resetToRecordStart(messageTypes);
    	//if any partial write of field data is in progress just throw it away because 
    	//next frame will begin again from the start of the message.
    	bytePosStartField = bytePosActive = RingBuffer.bytesHeadPosition(ringBuffer);
    	RingBuffer.abandonWrites(ringBuffer);
    	    	
        //get new catalog if is has been changed by the other visitor
        byte[] catBytes = messageTypes.getCatBytes();      
        
        
        //upon this new frame confirm that the catalog is still valid
        if (!Arrays.equals(this.catBytes, catBytes)) {
            this.catBytes = catBytes;  //replace old cat bytes so they can be requested      
            
            if (RingBuffer.from(ringBuffer).fieldNameScript[0].equals("catalog")) {
            	System.err.println("Wrote new catalog to stream for hand off"); 
            	RingBuffer.setValue(ringBuffer.structuredLayoutRingBuffer, ringBuffer.mask, RingBuffer.getWorkingHeadPositionObject(ringBuffer).value++, CATALOG_TEMPLATE_ID);        
            	RingBuffer.addByteArray(catBytes, 0, catBytes.length, ringBuffer);
            	RingBuffer.publishWrites(ringBuffer);
            } else {
            	//catalog write not supported
            	System.err.println("NO CAT SUPPORTED");
            	
            }
            
            //TODO: now out of sync so ring buffer must be shut down.
            //TODO: shut down this ring buffer after this point
            ringBuffer = null;
           
            throw new UnsupportedOperationException("Not done yet");
          
        }        
    }

	public void closeResults() {
		
		isAlive = false;		
		
	    	
        tailPosCache = RingBuffer.spinBlockOnTail(tailPosCache, headPosition(ringBuffer)-fill, ringBuffer);
		//TODO: AAA, need to send shutdown message to the stream
		//write message id for new stop message?
 //       //RingWriter.writeInt(ringBuffer, -1); //This is an invalid message idx because they are offsets.  
		
		RingBuffer.publishWrites(ringBuffer);
		
	}

	public long totalMessages() {
		return messageTypes.totalRecords;
	}

	@Override
	public void debug(ByteBuffer mappedBuffer, int startPos) {
		
		String example = new String(messageTypes.bytesForLine(mappedBuffer, startPos));
		System.err.println("found delimiter here {"+ example+"}" );
				
	}
	
	public static StreamingVisitor encodeToFAST(FileChannel fileChannel, byte[] catBytes, TemplateCatalogConfig catalog, FASTEncodeStage fes) throws IOException {
		
		TypeExtractor typeExtractor = new TypeExtractor(true /* force ASCII */);
		RecordFieldExtractor loadedSchema = new RecordFieldExtractor();
		loadedSchema.loadTemplate(catBytes, catalog, typeExtractor);	
		 
		byte primaryBits = 11;
		byte secondaryBits = 24;
		RingBuffer ringBuffer = new RingBuffer(new RingBufferConfig(primaryBits, secondaryBits, catalog.ringByteConstants(), catalog.getFROM()));
				
		StreamingVisitor streamingVisitor = new StreamingVisitor(loadedSchema, catalog.getTemplateStartIdx() , ringBuffer, typeExtractor);
				
		 
		int prevB = -1;
		int quoteCount = 0;
	    int fieldStart = 0;
		int recordStart = 0;
		 
		 byte[] quoter = new byte[256]; //these are all zeros
		 quoter['"'] = 1; //except for the value of quote.
		 
		 //setup for reading blocks from the file
		 int blockSize = 1<<28; //TODO: expose this
	     MappedByteBuffer sourceByteBuffer;	        
	     long fileSize = fileChannel.size();
	     long filePosition = 0;
	     sourceByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, filePosition, Math.min(blockSize, fileSize-filePosition));
		 //first block is now mapped and ready
	     
	     do {	     
		 
	    	 streamingVisitor.openFrame();				 
			 int position = sourceByteBuffer.position();
			 int limit = sourceByteBuffer.limit();
			 int pageSize = limit-position;
			 while (position<limit) {
			//	 System.err.println("position:"+position+" "+limit+" "+sourceByteBuffer.capacity());
				 					    		
			    		int b = sourceByteBuffer.get(position);
			    							    		
			    		//LF   10     00001010
			    		//CR  13      00001101
			    		//,       44  00101100
			    							    		
			    		//looking for LF, CR or comma
			    		if (0x08 == (b & 0b11011000) ) {
			    			if (','==b) {
			    				if ('\\' == prevB || (quoteCount&1)!=0) {
			    					//content not field
			    					
			    				
			    				} else {
			    					
			    					streamingVisitor.appendContent(sourceByteBuffer, fieldStart, position, false);
			    					streamingVisitor.closeField(sourceByteBuffer, recordStart);
		
			    					fieldStart = position+1;
			    				}
			    			} else {
			    				if (0b00001000 == (b & 0b11111000) ) {
				    				if ('\\' == prevB || (quoteCount&1)!=0) {
				    					//content not EOM because
				    					//it is escaped or inside a quote
				    				
				    				} else {
				    					
				    					streamingVisitor.appendContent(sourceByteBuffer, fieldStart, position, false);
				    					streamingVisitor.closeField(sourceByteBuffer, recordStart);
				    					streamingVisitor.closeRecord(sourceByteBuffer, recordStart);
				    					
				    					recordStart = position+1;
				    					fieldStart = position+1;
				    					
				    					FASTEncodeStage.encodeUntilRingEmpty(fes);
				    					//EOM
				    				}
			    				} else {
			    					//not something we want	
		
						    		quoteCount += quoter[0xFF&b];
			    				}
			    			}
			    		}
			    		
					prevB = b;
			    	position ++;						 
				 
			 }
			 filePosition += pageSize;
			 //one last record
			 if (position>recordStart) {
				 
				streamingVisitor.appendContent(sourceByteBuffer, fieldStart, position, false);
				
				if (filePosition>=fileSize) {
					System.err.println("REMOVE THIS IT HAS BEEN TESTED 777");
					streamingVisitor.closeField(sourceByteBuffer, recordStart);
					streamingVisitor.closeRecord(sourceByteBuffer, recordStart);
				}
				System.err.println("REMOVE THIS IT HAS BEEN TESTED 888");
			 }
			 			 
			 streamingVisitor.closeFrame();
			 
			 
			 sourceByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, filePosition, Math.min(blockSize, fileSize-filePosition));
	     } while (filePosition<fileSize);
		 
		 
		 FASTEncodeStage.encodeUntilRingEmpty(fes);
		 		
		return streamingVisitor;
	}



	public static StreamingVisitor encodeToFAST(final ByteBuffer sourceByteBuffer, byte[] catBytes,	TemplateCatalogConfig targetCatalog, RingBuffer targetRingBuffer, FASTEncodeStage fes) {
		
		 TypeExtractor typeExtractor = new TypeExtractor(true /* force ASCII */);
         RecordFieldExtractor loadedSchema = new RecordFieldExtractor();
		 loadedSchema.loadTemplate(catBytes,targetCatalog, typeExtractor);		
		 StreamingVisitor streamingVisitor = new StreamingVisitor(loadedSchema, targetCatalog.getTemplateStartIdx(), targetRingBuffer, typeExtractor);
		 		 
		 
		 //parse to fields 
		 //then to messages
		 
		 streamingVisitor.openFrame();				 
		 
		 int prevB = -1;
		 int quoteCount = 0;
	     int fieldStart = 0;
	     int recordStart = 0;
		 
		 byte[] quoter = new byte[256]; //these are all zeros
		 quoter['"'] = 1; //except for the value of quote.
		 
		 int position = sourceByteBuffer.position();
		 int limit = sourceByteBuffer.limit();
		 while (position<limit) {
			 					    		
		    		int b = sourceByteBuffer.get(position);
		    							    		
		    		//LF   10     00001010
		    		//CR  13      00001101
		    		//,       44  00101100
		    							    		
		    		//looking for LF, CR or comma
		    		if (0x08 == (b & 0b11011000) ) {
		    			if (','==b) { 
		    				if ('\\' == prevB || (quoteCount&1)!=0) {
		    					//content not field
		    					
		    				
		    				} else {
		    					
		    					streamingVisitor.appendContent(sourceByteBuffer, fieldStart, position, false);
		    					streamingVisitor.closeField(sourceByteBuffer, recordStart);
	
		    					fieldStart = position+1;
		    				}
		    			} else {
		    				if (0b00001000 == (b & 0b11111000) ) {
			    				if ('\\' == prevB || (quoteCount&1)!=0) {
			    					//content not EOM because
			    					//it is escaped or inside a quote
			    				
			    				} else {
			    					
			    					streamingVisitor.appendContent(sourceByteBuffer, fieldStart, position, false);
			    					streamingVisitor.closeField(sourceByteBuffer, recordStart);
			    					streamingVisitor.closeRecord(sourceByteBuffer, recordStart);
			    					
			    					
			    					try{
			    						FASTEncodeStage.encodeUntilRingEmpty(fes);
			    					} catch (Exception e) {
			    						
			    						String lastData = new String(streamingVisitor.messageTypes.bytesForLine(sourceByteBuffer, recordStart, position+1));
			    						
			    						System.err.println("lastData:"+lastData);
			    						e.printStackTrace();
			    						
			    						System.exit(-1);
			    						
			    						
			    					}
			    					recordStart = position+1;
			    					fieldStart = position+1;
			    					//EOM
			    				}
		    				} else {
		    					//not something we want	
	
					    		quoteCount += quoter[0xFF&b];
		    				}
		    			}
		    		}
		    		
				prevB = b;
		    	position ++;						 
			 
		 }
		 //one last record
		 if (position>recordStart) {
			 
			streamingVisitor.appendContent(sourceByteBuffer, fieldStart, position, false);
			streamingVisitor.closeField(sourceByteBuffer, recordStart);
			streamingVisitor.closeRecord(sourceByteBuffer, recordStart);
			
		 }
		 
		 sourceByteBuffer.position(position);
		 
		 streamingVisitor.closeFrame();
		 
		 FASTEncodeStage.encodeUntilRingEmpty(fes);
		 		
		return streamingVisitor;
	}



}
