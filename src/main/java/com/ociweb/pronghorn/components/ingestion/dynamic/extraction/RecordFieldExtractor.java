package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.BloomCountVisitor;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.BloomCounter;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.Huffman;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.HuffmanTree;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.HuffmanVisitor;
import com.ociweb.pronghorn.components.ingestion.dynamic.util.MurmurHash;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.schema.generator.CatalogGenerator;
import com.ociweb.pronghorn.pipe.schema.generator.FieldGenerator;
import com.ociweb.pronghorn.pipe.schema.generator.ItemGenerator;
import com.ociweb.pronghorn.pipe.schema.generator.TemplateGenerator;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.Histogram;

public class RecordFieldExtractor {

	/*
	 * Rules for generating the FAST template catalog xml
	 * 
	 * Attributes:
	 * 
	 * id - Everything must have an ID
	 *      The ID is the value that appears on the FAST encoded stream
	 *      The ID never appears on the ring buffer
	 *      The ID is huffman encoded based on the frequency of the messages
	 *      
	 * name - Human readable name for this field/message
	 *        Name should be auto generated when one is not supplied
	 *        Name is always the local name and does not contain outer scope data
	 *       
	 * dictionary - Identification for unique dictionaries used or shared by messages
	 * 				Always auto generated here and only applicable at the message/template level
	 * 				Multiple message may share value (TODO: X, but not yet supported)
	 *              value is always recorded in lower case hex
	 *              value is prepended by the following to indicate that the dictionary...
	 *               x - is determined by the tri msg pos id alone
	 *               h - is determined by the tri msg pos id AND the hash of the first column            
	 *               Other leading flags may be used to indicate hashing on other columns (TODO: X, add more flags)
	 * 
	 * On the ring buffer the leading int for message identification is defined as the offset 
	 * in the FROM script where the message starts.  This value is not found within the template however 
	 * it is indirectly derived from the template file as it is loaded.  The order of the messages 
	 * in the template file and addition/removal of fields in the template file will modify that value.
	 * As a result one must never persist the raw data on the ring buffer unless they also save the catalog.
	 * 
	 * By design the ring buffer is a transient data structure.  For durable persistence a specific stage 
	 * should be dedicated to that task. 
	 * 
	 */

    
    public static final int TYPE_TOP_EXCLUSIVE = 8;
    
    private static final int TYPE_NAME_ID = 11;
    private static final int TYPE_COUNTS_LEN = 12;
    private static final int TYPE_COUNTS = 13;
    private static final int TYPE_MIDX = 14; //template id and or hashed template id
    private static final int TYPE_EOM = 15;
    
    //                                              
    private static final int[] SUPER_TYPE = new int[]{
    	                        TypeExtractor.TYPE_ULONG,   //FROM TYPE_UINT
    	                        TypeExtractor.TYPE_SLONG,   //FROM TYPE_SINT
    	                        TypeExtractor.TYPE_DECIMAL, //FROM TYPE_ULONG
    	                        TypeExtractor.TYPE_DECIMAL, //FROM TYPE_SLONG
    	                        TypeExtractor.TYPE_BYTES,   //FROM TYPE_ASCII 
    	                        TypeExtractor.TYPE_BYTES,   //CANT CONVERT, THIS IS EVERYTHING
    	                        TypeExtractor.TYPE_ASCII,   //FROM TYPE_DECIMAL
    	                        TypeExtractor.TYPE_NULL,    //CANT CONVERT, THIS IS NOTHING
    };

    //16 possible field types
    
    private static final int   TYPE_STEP_SIZE = 16;
    
    
    private static final int HIGH_BIT_MSG_IDX_SHIFT = 31;
	private static final int HIGH_BIT_MSG_IDX = 1<<HIGH_BIT_MSG_IDX_SHIFT;
	private static final int HIGH_TWO_BC_ID = 3<<30;
	
	private static final int ID_MASK = (1<<30)-1;
	
	private int activeFieldHash;
	float abovePct = .86f;
    
    
    
    //TODO: need to trim leading white space for decision
    //TODO: need to keep leading real char for '0' '+' '-'
    
    //TODO: next step stream this data using next visitor into the ring buffer and these types.

    //Need flag to turn on that feature
    ///TODO: convert short byte sequences to int or long
    ///TODO: treat leading zero as ascii not numeric.
    
    
    
    //TODO: for each long value must keep it in the field location 
    //      need to look this up for the next time arround by hash??
    //
    
    //TODO: unrelated to this code but a sort stage that takes a ring buffer may be a big help to the compression process may only work on bytes?? or extern structs

	private int          hashedRecordCollectionId; 
    private int          hashedRecordHash;

    
    private static final int   typeTrieArraySize = 1<<21; //2M
    private static final int   typeTrieArrayMask = typeTrieArraySize-1;
    
    public static final int   OPTIONAL_SHIFT = 30;
    public static final int   OPTIONAL_FLAG  = 1<<OPTIONAL_SHIFT;
    private static final int   OPTIONAL_LOW_MASK  = (1<<(OPTIONAL_SHIFT-1))-1;
    
    private static final int   CATALOG_TEMPLATE_ID = 0;
    
    private final int[] typeTrie = new int[typeTrieArraySize];
    int         typeTrieCursor; 
    int         typeTrieCursorPrev;
    
    public int                 fieldCount;
    
    int                 maxFieldCount;
    
    private int         typeTrieLimit = TYPE_STEP_SIZE;
    
    public long totalRecords;
    long tossedRecords;
    
    byte[] catBytes;
    
    static final int BITS_FOR_MAX_UNIQUE_TEMPLATES = TokenBuilder.MAX_FIELD_ID_BITS-1;// must be 1 less than that supported in the jFAST encoder so we have room    
    public final int someSeed = 31;
    final int bloomId = 42;
    public BloomCounter bc = new BloomCounter(BITS_FOR_MAX_UNIQUE_TEMPLATES, someSeed, bloomId);
    
    //Must check in the parser
    ///TODO: AA, need this to be smaller for most cases but very large for a few.
    ///TODO: AA, second encode pass must not use this memory
    public final static int MAX_SUPPORTED_FIELDS = 32; //NOTE: probably can not do more than 1000 or so fields without a redesign   
  
    //as long as we support fewer templates with more fields this constant does not matter so much
    
    private static int fieldsToKeepForAnalysis = (1<<BITS_FOR_MAX_UNIQUE_TEMPLATES)*MAX_SUPPORTED_FIELDS;
    private static int fieldsToKeepInBits = 32 - Integer.numberOfLeadingZeros(fieldsToKeepForAnalysis - 1);
    
    static {
    	
    	System.err.println("fields to keep in bits "+fieldsToKeepInBits);    	
    }
    
    private static int keptMask = (1<<fieldsToKeepInBits)-1;
    private int 	   keptIndex = 0;
    private int[]      keptFieldHash = new int[1<<fieldsToKeepInBits];
    private long[]     keptFieldLong = new long[1<<fieldsToKeepInBits];
    private int[]      keptIndexLookup = new int[1<<fieldsToKeepInBits];
    private int        keptStart;
    
    
    //TODO: two records that start with the same key but have different lengths will end up getting compared to one another.
    //      this is a serious problem to be solved, Those messages must be grouped or we need another key.
    //      NOTE: this must be big enough to hold the largest number of records expected. as we count each instance. TODO: AAA, need to make smaller.
    private static int MAX_TOTAL_FIELDS = TYPE_TOP_EXCLUSIVE * MAX_SUPPORTED_FIELDS*1000;

    //these together are for computing delta, bytes used are values 1 - 9
    private static final int MAX_BYTES_IN_LONG = 10;
    
    //TODO: AA, need to use less memory, this can be done by keeping the total instead of the historgram here!! 
    private int[][] valueHistCounts = new int[MAX_TOTAL_FIELDS][MAX_BYTES_IN_LONG];
    private int[][] deltaHistPrevCounts = new int[MAX_TOTAL_FIELDS][MAX_BYTES_IN_LONG];
    private int[][] copyHistCounts = new int[MAX_TOTAL_FIELDS][MAX_BYTES_IN_LONG];
    private int[][] incHistCounts = new int[MAX_TOTAL_FIELDS][MAX_BYTES_IN_LONG];
    
    
    private int intCountsLimit = 1;
    
    public int[] validMessageIdx = new int[TokenBuilder.MAX_FIELD_ID_VALUE];  
    
    ////////////////////////////////////////
    ///DEBUG FEATURE THAT LOGS THE FIRST FEW EXAMPLES THAT CAUSED A NEW TEMPLATE
    ////////////////////////////////////////
    int reportLimit = 0; //turn off the debug feature by setting this to zero.
    ////////////////////////////////////////
    
    
    public RecordFieldExtractor() {    	
              
		resetToRecordStart(this);
        
    }
    
    public static int activeFieldHash(RecordFieldExtractor rfe) {
    	return rfe.activeFieldHash;
    }
    
    public static void activeFieldHash(RecordFieldExtractor rfe, int hash) {
    	rfe.activeFieldHash = hash;
    }
    
	public static void appendNewRecord(RecordFieldExtractor rfe,
			ByteBuffer buffer, int startPos) {
		int total = appendNewRecord(rfe);
        
        if (total<=rfe.reportLimit) {
            if (buffer.position()-startPos<200) { 
                
                System.err.println("example "+total+" for :"+(rfe.typeTrieCursor+TYPE_EOM));
                
                byte[] dst = rfe.bytesForLine(buffer, startPos);
                System.err.println(new String(dst));     
            } else {
                System.err.println(startPos+" to "+buffer.position());
            }            
        }

		RecordFieldExtractor.resetToRecordStart(rfe);
	}

	public static int appendNewRecord(RecordFieldExtractor rfe) {
    	
    	//use hashedRecordCollectionId to look up the last kept
		int keptRecIdx = ID_MASK & rfe.hashedRecordHash;
    	int lastKeptIdx = rfe.keptIndexLookup[keptRecIdx];    
    	int cursor = rfe.typeTrieCursor;
    	int countsIdx = rfe.typeTrie[cursor+TYPE_COUNTS];
    	if (0 == countsIdx) {    		
    		//System.err.println(intCountsLimit+" "+fieldCount);
    		rfe.typeTrie[cursor+TYPE_COUNTS] = countsIdx = rfe.intCountsLimit;
    		rfe.typeTrie[cursor+TYPE_COUNTS_LEN] = rfe.fieldCount;
    		//make room for this records fields so the next one will continue after this point.
    		rfe.intCountsLimit += rfe.fieldCount;
    		
    		if (rfe.intCountsLimit> (1<<30)) {
    			System.err.println("field counts limit:"+rfe.intCountsLimit);
    		}
    	}
    	
    	int localKeptStart = rfe.keptStart;
    	int i = rfe.fieldCount;
    	while (--i>=0) {
    		final int  prvIdx = keptMask&(lastKeptIdx+i);
    		final int  nxtIdx = keptMask&(localKeptStart+i);

    		//this value is the numeric long or the length for ASCII or ByteArrays
    		long value = rfe.keptFieldLong[nxtIdx];
    		long prevValue = rfe.keptFieldLong[prvIdx];
    		
    		int bytesNeededToStoreValue = rfe.bytesNeededToStoreValue(value);
    		int idx = countsIdx+i;
			if (rfe.keptFieldHash[prvIdx]==rfe.keptFieldHash[nxtIdx]) { 
				rfe.copyHistCounts[idx][bytesNeededToStoreValue]++;
			}
    		if ((1+prevValue)==value) {
    			rfe.incHistCounts[idx][bytesNeededToStoreValue]++;
    		}   	//TODO: BB, re-evaluate this to do more work and take less memory.	
    		
    		int[] dhpc = rfe.deltaHistPrevCounts[idx];
    		//bytes needed to store always returns values 1 -> 9
			dhpc[rfe.bytesNeededToStoreValue(value - prevValue)]++;
    		    
    		//this is also recombined later to produce the total counts
    		rfe.valueHistCounts[idx][bytesNeededToStoreValue]++;    		    		
    	}
    	   	    	
    	//store  	keptStart  at  hashedRecordCollectionId
    	rfe.keptIndexLookup[keptRecIdx] = localKeptStart;    	    	
    	rfe.totalRecords++;                
    	rfe.typeTrie[cursor+TYPE_MIDX] =  rfe.hashedRecordCollectionId;              
        
    	
    	if (rfe.typeTrie[cursor+TYPE_EOM]>(1<<30)) {
    		System.err.println("Message counts:"+rfe.typeTrie[cursor+TYPE_EOM]);
    	}
    	
        return  ++rfe.typeTrie[cursor+TYPE_EOM];
	}

	private int bytesNeededToStoreValue(long value) {
		//  0==value||1==value
		return 0==(value>>>1) ? 1 :  (6+(64 - Long.numberOfLeadingZeros(value>0 ? value-1 : 0-value)))/7; ///lookup table instead of division?
	}


	public byte[] bytesForLine(ByteBuffer buffer, int startPos) {
		int lim = buffer.position();// Math.min(tempBuffer.position()+50, tempBuffer.limit());
		
		return bytesForLine(buffer, startPos, lim);
	}

	public byte[] bytesForLine(ByteBuffer buffer, int startPos, int lim) {
		byte[] dst = new byte[lim-startPos];
		ByteBuffer x = buffer.asReadOnlyBuffer();
		x.position(startPos);
		x.limit(lim);
		x.get(dst, 0, dst.length);
		return dst;
	}

	public static void appendNewField(RecordFieldExtractor rfe, int type, long toKeep, int optionalFlag, Pipe nameBuffer, int loc) {
		//TODO: A, Do something with the name. keep so the name can be re-used for export.
		
		//hash this name with the current location.
		//rfe.fieldCount
		int pos = PipeReader.readBytesPosition(nameBuffer, loc);
		int len = PipeReader.readBytesLength(nameBuffer, loc);
		byte[] src = PipeReader.readBytesBackingArray(nameBuffer, loc);
		
		//top 32 hash of string, next 16 length of string, last 16 column position
		long nameHash = ((long)MurmurHash.hash32(src, pos, len, nameBuffer.byteMask, rfe.someSeed))<<32 | (((long)len)<<16) | (long)rfe.fieldCount;
		
		//TODO: AA, check if this matches another field if so they can be combined if desired.
		
		//TODO: AA, keep each unquie hash with a string value, if there is a collision note it here.
		
		appendNewField(rfe,type,toKeep,optionalFlag);
	}
	
    public static void appendNewField(RecordFieldExtractor rfe, int type, long toKeep, int optionalFlag) {
    	int off = keptMask & rfe.keptIndex;
		rfe.keptFieldHash[off] = rfe.activeFieldHash;
    	rfe.keptFieldLong[off] = toKeep;
    	
    	rfe.keptIndex++;
    	
    	//TODO: need to store the int for this field.
    	//TODO: need to store the array for the copy check.
    	//if numeric store the value else store the hash
    	//dont know the field position until the record is chosen, only known and new record. until then all the values must be kept somewhere.
    	//keep giant array, and free index, store this index with the hash so it knows where to look, its differnt every time.
    	//we always write forward!!!
    	
        ++rfe.fieldCount;

        //upconverting logic
        switch (type) {
        	case TypeExtractor.TYPE_ASCII:
        		if (1==rfe.fieldCount) { 
        			//Assumes primary key can not be escaped and therefore will only come in one block 
        			//Assumes primary key can not be optional and will always have content.
        			//Assumes alpha key will always contain some alpha char
        			//Assumes primary key can only be found in the first position.
        			
        			BloomCounter.sample(rfe.activeFieldHash, rfe.bc);
        			rfe.hashedRecordCollectionId =  HIGH_TWO_BC_ID | rfe.bc.id; //set high bits to set it apart from loaded templateId and generated teamplateId
        			rfe.hashedRecordHash = rfe.activeFieldHash & rfe.bc.mask;
        			
        		}
        
        	break;	
        	case TypeExtractor.TYPE_UINT:
        		//switch up to long if it is already in use
        		if (rfe.typeTrie[TypeExtractor.TYPE_ULONG+rfe.typeTrieCursor]!=0) {
        			type = TypeExtractor.TYPE_ULONG;
        		}   
        		
//            //TODO: this is a hack for now we need to do a recursive clean up of small message counts into bigger message counts
        		if ((rfe.typeTrie[TypeExtractor.TYPE_ASCII+rfe.typeTrieCursor]&OPTIONAL_FLAG)!=0 && rfe.fieldCount==2) {
        			type = TypeExtractor.TYPE_ASCII;
        		}   
        		
        		
        	break;
        	case TypeExtractor.TYPE_SINT:
        		//switch up to long if it is already in use
        		if (rfe.typeTrie[TypeExtractor.TYPE_SLONG+rfe.typeTrieCursor]!=0) {
        			type = TypeExtractor.TYPE_SLONG;
        		}            
            break;
        	case TypeExtractor.TYPE_NULL:
        		//if null comes in convert it to another type if possible
        		//if there is only 1 other type the use it
        		int lastNonNull = lastNonNull(rfe.typeTrieCursor, rfe.typeTrie, TYPE_TOP_EXCLUSIVE);
        		
        		if (lastNonNull>=0) {
        			if ((lastNonNull(rfe.typeTrieCursor, rfe.typeTrie, lastNonNull)<0)) {
        				
        				//there is only 1 non null use it
        				
        				type = lastNonNull;
        				rfe.typeTrie[rfe.typeTrieCursor+type] |= OPTIONAL_FLAG;        		
        			} else {
        				
        				//here are mupliple choices
        				//the best bet is to pick the one that happens most often
        				
        				int bestIdx = lastNonNull;
        				int bestCount = recordCount(rfe.typeTrieCursor+lastNonNull, rfe.typeTrie, 0);
        				
        				while (lastNonNull>=0) {
        					lastNonNull = lastNonNull(rfe.typeTrieCursor, rfe.typeTrie, lastNonNull);
        					if (lastNonNull>=0) {
        						int count = recordCount(rfe.typeTrieCursor+lastNonNull, rfe.typeTrie, 0); 
        						if (count>bestCount) {
        							bestCount = count;
        							bestIdx = lastNonNull;
        						}        				
        					}
        				}
        				type = bestIdx;
        				rfe.typeTrie[rfe.typeTrieCursor+type] |= OPTIONAL_FLAG;        		
        				
        			}
        		}
        	break;
        }

        
        //store type into the Trie to build messages.
        rfe.appendNewField(type, optionalFlag);
	}

	private String recentDataContext(ByteBuffer buffer) {
		return new String(bytesForLine(buffer, Math.max(0, buffer.position()-100)));
	}

	private void appendNewField(int type, String name, int optionalFlag) {
//TODO: AAA, still under developmnent
//		if (null!=name && name.length()>0) {
//			typeTrie[typeTrieCursor+TYPE_NAME_ID] = addName(name);
//		}
		appendNewField(type,optionalFlag);
	}
	
	private void appendNewField(int type, int optionalFlag) {
		int pos = typeTrieCursor + (OPTIONAL_LOW_MASK & type);
               
		int tmp = typeTrie[pos];
        if (tmp!=0) {
        	//use existing position
        	typeTrie[pos] = optionalFlag | tmp;
        	typeTrieCursor = OPTIONAL_LOW_MASK & tmp;
        } else {
        	
        	//create new position          
        	typeTrie[pos] =  (OPTIONAL_FLAG & type) | (typeTrieCursor = typeTrieLimit) | optionalFlag;
        	typeTrieLimit += TYPE_STEP_SIZE;
        	//System.err.println("new limit:"+typeTrieLimit);
        }
	}
    
	public static int moveNextField(RecordFieldExtractor rfe, int type, int optionalFlag, Pipe nameBuffer, int loc) {		
		
		//hash this name with the current location.
		//rfe.fieldCount
		int pos = PipeReader.readBytesPosition(nameBuffer, loc);
		int len = PipeReader.readBytesLength(nameBuffer, loc);
		byte[] src = PipeReader.readBytesBackingArray(nameBuffer, loc);
		
		//top 32 hash of string, next 16 length of string, last 16 column position
		long nameHash = ((long)MurmurHash.hash32(src, pos, len, nameBuffer.byteMask, rfe.someSeed))<<32 | (((long)len)<<16) | (long)rfe.fieldCount;
		
		//TODO: check the name
		
		
		return moveNextField(rfe,type,optionalFlag);
	}
	
    public static int moveNextField(RecordFieldExtractor rfe, int type, int optionalFlag) {
		rfe.fieldCount++; 
		
		if (TypeExtractor.TYPE_NULL == type) {
			optionalFlag = OPTIONAL_FLAG;
			type = rfe.convertRawTypeToSpecific(type);
		}
		
		rfe.typeTrie[rfe.typeTrieCursor+type] |= optionalFlag;
		
        //TODO: add optional flag, ensure an optional choice is made.
        int nextIdx = OPTIONAL_LOW_MASK & rfe.typeTrie[rfe.typeTrieCursor+type];
        if (0==nextIdx) {        
        	int origType = type;
	        type = rfe.convertRawTypeToSpecific(type); //TODO: convert to static or private
	        //System.err.println("started with "+origType+" and became "+type);
	        
	        nextIdx = OPTIONAL_LOW_MASK & rfe.typeTrie[rfe.typeTrieCursor+type];
	        if (0==nextIdx) {
	        	
	        	int countOfNonZero = 0;
	        	int lastNonZero = -1; 
	        	
	        	
	        	int j = TYPE_TOP_EXCLUSIVE;
	        	while (--j>=0) {
	        		if (0 != rfe.typeTrie[rfe.typeTrieCursor+j]) {
	        			countOfNonZero++;
	        			lastNonZero = j;
	        		}
	        		
	        		//System.err.println("idx:"+j+" base:"+typeTrieCursor+" value:"+(typeTrie[typeTrieCursor+j]));
	        	}
	        	if (countOfNonZero==1) {
	        		type = lastNonZero; //TODO: should never be an error if orig type was 7 but this  must now be optional?
	        		System.err.println("A WARN: assumed type of :"+type+" ************************* ERROR not found origType:"+origType+" field position "+rfe.fieldCount);
	        	} else {
	        		
	        		System.err.println("B WARN: assumed type of :"+type+" ************************* ERROR not found origType:"+origType+" field position "+rfe.fieldCount);
		        	System.err.println("  "+nextIdx+" from "+rfe.typeTrieCursor+"+"+type+" orig "+origType);
		        	
		        	//////////////////////////DOWNGRADES FROM UTF8 to ASCII//////////////////////////
		        	//If we picked bytes but there is none however we do have ASCII so we map to that
		        	//This is done because the template is already established and we have little choice
		        	/////////////////////////////////////////////////////////////////////////////////
		        	if (TypeExtractor.TYPE_BYTES==type && 0!=rfe.typeTrie[rfe.typeTrieCursor+TypeExtractor.TYPE_ASCII]) {
		        		type= TypeExtractor.TYPE_ASCII;    
		        	} 
	        	}	
	        }
        }        
        rfe.typeTrieCursorPrev = rfe.typeTrieCursor; //TODO: may not need this
        rfe.typeTrieCursor = nextIdx;
      //  System.err.println("last picked type:"+type+"  "+rfe.typeTrieCursor+"  "+Arrays.toString(Arrays.copyOfRange(rfe.typeTrie, rfe.typeTrieCursor, rfe.typeTrieCursor+8))+" prev "+rfe.typeTrieCursorPrev);
		return type;
	}
    
    

    /**
     * Only called after the record and types have been established from the first pass.
     * This is needed to map the raw extract types to the final determined types after
     * the full analysis at the end of the last iteration.
     * 
     * @param type
     * @return
     */
	public int convertRawTypeToSpecific(int type) {
		////////TYPE CHANGE IS DONE HERE BECAUSE THE TEMPLATE IS FIXED AND WE MUST PARSE INTO IT//////////////

		
		int initType = type;
				
		//if type is Null find the most appropriate choice
        if (TypeExtractor.TYPE_NULL == type) {            
            //select the first optional type instead of null

            int i = TYPE_TOP_EXCLUSIVE;
            int fieldsCount = 0;
            int fieldsLastType = -1;
            while (--i>=0) {
                if (TypeExtractor.TYPE_NULL != i) {  
                	int value = typeTrie[typeTrieCursor+i];
                    if (0 != (OPTIONAL_FLAG & value)) {   
                    	//return the first null type
                    	return i; 
                    }               
                    //count fields if we only have 1 and if that 1 is numeric and not optional do use it with zero.
                    if (0 != value) {   
                    	fieldsCount++;
                    	fieldsLastType = i;
                    }                   
                }   
            }
            if (0==fieldsCount) {
            	throw new UnsupportedOperationException("reached dead end of message definition.");
            }
            //
            //no null-able field found above
            //
            if (1==fieldsCount) {
            	//There is only 1 choice so take it
            	typeTrie[typeTrieCursor+fieldsLastType] |= OPTIONAL_FLAG;
            	return fieldsLastType;
            }            
            //
            //a single field was not found, try assigning it to Bytes or ASCII
            //
            if (typeTrie[typeTrieCursor+TypeExtractor.TYPE_BYTES]!=0) {
            	typeTrie[typeTrieCursor+TypeExtractor.TYPE_BYTES] |= OPTIONAL_FLAG;
            	return TypeExtractor.TYPE_BYTES;
            }
            if (typeTrie[typeTrieCursor+TypeExtractor.TYPE_ASCII]!=0) {
            	typeTrie[typeTrieCursor+TypeExtractor.TYPE_ASCII] |= OPTIONAL_FLAG;
            	return TypeExtractor.TYPE_ASCII;
            }
            if (typeTrie[typeTrieCursor+TypeExtractor.TYPE_DECIMAL]!=0) {
            	typeTrie[typeTrieCursor+TypeExtractor.TYPE_DECIMAL] |= OPTIONAL_FLAG;
            	return TypeExtractor.TYPE_DECIMAL;
            }           
            
            throw new UnsupportedOperationException("reached dead end of message definition.");

        } else {

    		//don't care about previous records count, if the path exists then take it
    		if (0!=typeTrie[typeTrieCursor+type]) {
    			return type;
    		}

    		
        	//if the type is not found then bump it up until one is found starting from this known type.
        	while (0==typeTrie[typeTrieCursor+type] ) {
        		//bump up type
        		int newType = SUPER_TYPE[type];  
                if (type==newType) {
                	break;
                }
        		type = newType;      		        		
        	}
        	
        }
        
        //System.err.println("new type "+type+" from type "+initType+" in "+Arrays.toString(Arrays.copyOfRange(typeTrie, typeTrieCursor, typeTrieCursor+8))+" from "+typeTrieCursor+" fieldPos "+fieldCount);
        
        return type;
	}
	
	  
	
    public int messageIdx(int messageTemplateIdHash) {
    	
    	int result =  typeTrie[typeTrieCursor+TYPE_MIDX];

    	//high bit must be set or we have a configuration error someplace
    	if ((result&HIGH_BIT_MSG_IDX) != 0) {
    		//high two bits set indicates that this message does NOT use a constant hash to determine the template id.
    		if ((result&(HIGH_BIT_MSG_IDX>>>1)) == 0) {
    			//contains the actual message idx because the top two bits are 10
    			return ID_MASK&result;
    		} else {
    			//top two bits are 11 so we must look up the location from the hash code    			
    			int temp = validMessageIdx[messageTemplateIdHash]; //TODO: A, this would be better as the hash map
    			if (0 != temp) {
    				//this case is expected to happen many more times than the second case
    				return temp-1;
    			} else {
    				//this hash was not frequent enough to deserve its own dictionary
    				//result still contains the msgIdx for the default case so we use it.
    				return ID_MASK&result; //default case for rare hashes to collide
    			}
    		}
    	} else {
    		throw new RuntimeException("Message stopped where there is no template: "+(typeTrieCursor+TYPE_MIDX));
    	}  	
    }
    
    
    public static void resetToRecordStart(RecordFieldExtractor rfe) {
		rfe.typeTrieCursor = 0;        
    	rfe.maxFieldCount = Math.max(rfe.maxFieldCount, rfe.fieldCount);
    	rfe.fieldCount = 0;
    	rfe.hashedRecordCollectionId = 0;
    	rfe.hashedRecordHash = 0;
    	rfe.keptStart = rfe.keptIndex;
	}
    
    //if all zero but the null then do recurse null otherwise not.
    
    public void printRecursiveReport(int pos, String tab) {

    	//some messages are subsets of larger messages, if a message ends here we mark it as special
    	String isEndOfMsg = "";
    	if ( (OPTIONAL_LOW_MASK&typeTrie[pos+TYPE_EOM]) > 0) {
    		isEndOfMsg = "EOM";
    	}
    	int i = TYPE_TOP_EXCLUSIVE;
        while (--i>=0) {
            int value = OPTIONAL_LOW_MASK&typeTrie[pos+i];
            if (value > 0) {                
                    if (notEmpty(value, typeTrie, 0)) {                    
                        
                        int type = i<<1;
                        if (type<TypeMask.methodTypeName.length) {                    
                            
                            
                            String v = (i==TypeExtractor.TYPE_NULL ? "NULL" : TypeMask.methodTypeName[type]);
                            
                            if ((OPTIONAL_FLAG&typeTrie[pos+i])!=0) {
                                v = "Optional"+v;
                            }
                            
                            v = v+" ["+pos+"..."+(pos+8)+"] "+isEndOfMsg;
                            
                            
                         //   noOutput = false;
                            
                            System.err.println(tab+v);
                            printRecursiveReport(value, tab+"     ");    
                        }
                    }
 
            }
        }       

        
    }
    
    

    private static boolean notEmpty(int value, int[] typeTrie, int count) {

        if ((OPTIONAL_LOW_MASK&typeTrie[value+TYPE_EOM])>0) {
        	return true;
        }                
    	
        int i = TYPE_TOP_EXCLUSIVE;
        while (--i>=0) {
            int temp = OPTIONAL_LOW_MASK&typeTrie[value+i];
            if (temp>0) {
               	if (notEmpty(temp, typeTrie, count+1)) {
               		return true;
               	}
            }
        }
        return false;
    }
    
    private static int recordCount(int value, int[] typeTrie, int count) {

    	if (count>15) {
    		throw new RuntimeException("Too deep something has gone wrong. "+value);
    	}
    	int c = (OPTIONAL_LOW_MASK&typeTrie[value+TYPE_EOM]);               
    	        
        int i = TYPE_TOP_EXCLUSIVE;
        while (--i>=0) {
            int temp = OPTIONAL_LOW_MASK&typeTrie[value+i];
            if (temp>0) {
            	c += recordCount(temp, typeTrie, count+1);
            }
        }
        return c;
    }

    public void mergeOptionalNulls(int pos) {
    	
    	
        int i = TYPE_TOP_EXCLUSIVE;
        while (--i>=0) {
            int value = OPTIONAL_LOW_MASK&typeTrie[pos+i];
            if (value > 0) {                
                if (notEmpty(value, typeTrie, 0)) {
                    mergeOptionalNulls(value);
                    
                }        
            }            
        }  
        
        
    	boolean hasNullPath = 0!=(OPTIONAL_LOW_MASK&typeTrie[pos+TypeExtractor.TYPE_NULL]) && notEmpty(OPTIONAL_LOW_MASK&typeTrie[pos+TypeExtractor.TYPE_NULL], typeTrie, 0); 
    	
    	//clean up nulls on this level first
    	if (hasNullPath) {
    		int lastNonNull = lastNonNull(pos, typeTrie, TYPE_TOP_EXCLUSIVE);
    		
    		//check if there is another non-null field
    		//if there is more than 1 field with the null NEVER collapse because we don't know which path to which it belongs.
    		//we will produce 3 or more separate templates and they will be resolved by the later consumption stages
            if (lastNonNull>=0 && (lastNonNull(pos, typeTrie, lastNonNull)<0)) {
                
                    int nullPos = OPTIONAL_LOW_MASK&typeTrie[pos+TypeExtractor.TYPE_NULL];
                    int thatPosDoes = OPTIONAL_LOW_MASK&typeTrie[pos+lastNonNull];
                                                
                    //if recursively all of null pos is contained in that pos then we will move it over.                            
                    if (contains(nullPos,thatPosDoes)) {
                        //since the null is a full subset add all its counts to the larger
                        sum(nullPos,thatPosDoes);                                
                        //flag this type as optional
                        typeTrie[pos+lastNonNull] |= OPTIONAL_FLAG;
                        //remove the null NOT NEEDED BECAUSE SUM SET THIS CHAIN TO ZERO
                        typeTrie[pos+TypeExtractor.TYPE_NULL] = 0;
                        
                    } 

            }
    		
    	}
    	
    	//now go down each path
    	
    	//int lastNonNull = lastNonNull(pos, typeTrie, TYPE_TOP_EXCLUSIVE);
    	
    	
    	
    	
      
    }
    
   
     
    
    //If two numeric types are found and one can fit inside the other then merge them.
    public void mergeNumerics(int pos) { //TODO: rename as general merge
        
        int i = TYPE_TOP_EXCLUSIVE;
        while (--i>=0) {
            int value = OPTIONAL_LOW_MASK&typeTrie[pos+i];
            if (value > 0) {                
                if (notEmpty(value,typeTrie, 0)) {
                    mergeNumerics(value);
    
                    //finished call for this position i so it can removed if needed
                    //after merge on the way up also ensure we are doing the smallest parts first then larger ones
                    //and everything after this point is already merged.
                    
                    //uint to ulong to decimal
                    //sint to slong to decimal

                    mergeTypes(pos, i, value, TypeExtractor.TYPE_ULONG, TypeExtractor.TYPE_UINT); //UINT  into ULONG
                    mergeTypes(pos, i, value, TypeExtractor.TYPE_SLONG, TypeExtractor.TYPE_SINT); //SINT  into SLONG
                    
                    //TOOD: in the future may want to only do the merge if the sub type is only a small count of instances relative to the super type.         
                    mergeTypes(pos, i, value, TypeExtractor.TYPE_DECIMAL, TypeExtractor.TYPE_SINT); //SLONG into DECIMAL
                    mergeTypes(pos, i, value, TypeExtractor.TYPE_DECIMAL, TypeExtractor.TYPE_UINT); //SLONG into DECIMAL
                    mergeTypes(pos, i, value, TypeExtractor.TYPE_DECIMAL, TypeExtractor.TYPE_SLONG); //SLONG into DECIMAL
                    mergeTypes(pos, i, value, TypeExtractor.TYPE_DECIMAL, TypeExtractor.TYPE_ULONG); //SLONG into DECIMAL
                    
                }        
            }            
        }        
    }

    //TODO: this is not recursive and we need it do a deep contains and sum so we must extract the subset rules.
    private void mergeTypes(int pos, int i, int value, int t1, int t2) {
        int thatPosDoes = OPTIONAL_LOW_MASK&typeTrie[pos+t1];
        if (thatPosDoes>0 && t2==i) {
                                   
                
                //if recursively all of null pos is contained in that pos then we will move it over.                            
                if (contains(value,thatPosDoes)) {
                    //since the null is a full subset add all its counts to the rlarger
                    sum(value,thatPosDoes);   
                }

        }
    }
    
    
    //for each level if we find a null call merge
    //it if only 2 null and real then check that null children can fit in other
    //this may require null adjust on every child.
   
    
    private int targetType(int i, int targetset) {
        if (TypeExtractor.TYPE_NULL==i) {
            //if only one type is optional then we can represent this null as that type.
            //if there is more than one then we must not combine them until or if the types are merged first
            int q = TYPE_TOP_EXCLUSIVE;
            int result = -1;
            while (--q>=0) {
                if (TypeExtractor.TYPE_NULL!=q && 
                    notEmpty(OPTIONAL_LOW_MASK&typeTrie[targetset+q], typeTrie, 0)  &&
                    (0!=  (OPTIONAL_FLAG&typeTrie[targetset+q]))) {
                    
                        if (result<0) {
                            result = q;
                        } else {
                            return -1;
                        }  
                        
                }                
            }
            return result;
            
        } else {
        
        
            //if own kind is not found check for the simple super
            if (TypeExtractor.TYPE_UINT==i) {//TODO: EXPAND FOR SUPPORT OF SIGNED
                if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+TypeExtractor.TYPE_ULONG])) {
                    if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+TypeExtractor.TYPE_DECIMAL])) {
                        return -1;
                    } else {
                        return TypeExtractor.TYPE_DECIMAL;
                    }
                } else {
                    return TypeExtractor.TYPE_ULONG;
                }
            } else {
                
                if (TypeExtractor.TYPE_ULONG==i) {//TODO: EXPAND FOR SUPPORT OF SIGNED
                    if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+TypeExtractor.TYPE_DECIMAL])) {
                        return -1;
                    } else {
                        return TypeExtractor.TYPE_DECIMAL;
                    }
                } else {
                    return -1;
                }                          
                
            }
        }
    
    }

    private boolean contains(int subset, int targetset) {
        //if all the field in inner are contained in outer
        int i = TYPE_TOP_EXCLUSIVE;
        
        int nonNullCount = 0;
        boolean subsetHasNull = false;
        while (--i>=0) {
        	boolean subsetHas = 0!=(OPTIONAL_LOW_MASK&typeTrie[subset+i]) && notEmpty(OPTIONAL_LOW_MASK&typeTrie[subset+i], typeTrie, 0); 
        //	if (i!=TYPE_NULL) {
        		boolean targetHas = 0!=(OPTIONAL_LOW_MASK&typeTrie[targetset+i]) && notEmpty(OPTIONAL_LOW_MASK&typeTrie[targetset+i], typeTrie, 0);
        		
	        	//if the subset has this field type then the target must also have it
	            if (subsetHas) { 
	                int j = i;
	                if (!targetHas) {
	                    
	                    j = targetType(i,targetset);
	                    if (j<0) {
	                    	return false;
	                    }                                            
	                    
	                }                           
	                if (!contains(OPTIONAL_LOW_MASK&typeTrie[subset+i],
	                		      OPTIONAL_LOW_MASK&typeTrie[targetset+j])  ) {
	                    return false;
	                }
	            }
	            if (targetHas) {
	            	nonNullCount++;
	            }
	            if (i==TypeExtractor.TYPE_NULL) {//} else {
        		subsetHasNull = subsetHas;
          	}
        }         
        
        if (subsetHasNull) {
        	//can only combine with the target if there is only 1 so we know where it will be going.
        	if (nonNullCount > 1) {
        		return false;
        	}
        }
        
        return true;
    }
    
    private boolean sum(int subset, int targetset) {
        //if all the field in inner are contained in outer
        int i = TYPE_TOP_EXCLUSIVE;
             
        while (--i>=0) {
            int j = i;
            //exclude this type its only holding the count

            	boolean subsetHas = 0!=(OPTIONAL_LOW_MASK&typeTrie[subset+i]) && notEmpty(OPTIONAL_LOW_MASK&typeTrie[subset+i], typeTrie, 0); 
            	boolean targetHas = 0!=(OPTIONAL_LOW_MASK&typeTrie[targetset+i]) && notEmpty(OPTIONAL_LOW_MASK&typeTrie[targetset+i], typeTrie, 0);
	            	
	                if (subsetHas) {                    
	                    if (!targetHas) {                    
	                        
	                        j = targetType(i,targetset);
	                        if (j<0) {
	                        	System.err.println("should never happen filtered above **********************");
	                        	
	                            return false;
	                        }
	                        
	                    }
	                    
	                    if (!sum(OPTIONAL_LOW_MASK&typeTrie[subset+i],
	                    		 OPTIONAL_LOW_MASK&typeTrie[targetset+j])  ) {
	                        return false;
	                    }
	                }
		            
	                //target remains at the same location
	                //don't loose the optional flag from the other branch if it is there                
	                typeTrie[targetset+j]  = (OPTIONAL_FLAG & (typeTrie[subset+i] | typeTrie[targetset+j])) |
	                                         (OPTIONAL_LOW_MASK&(typeTrie[targetset+j]));

        }         
        //everything matches to this point so add the inner sum total into the outer
        typeTrie[targetset+TYPE_EOM]  = (OPTIONAL_LOW_MASK & (typeTrie[subset+TYPE_EOM] + typeTrie[targetset+TYPE_EOM]));
        typeTrie[subset+TYPE_EOM] = 0;//clear out old value so we do not double count
        
        //copy over all the counts because these two templates are merged.
        if (0 != typeTrie[subset+TYPE_COUNTS] || 0 != typeTrie[subset+TYPE_COUNTS_LEN] ) {
        
        	int fromBase = typeTrie[subset+TYPE_COUNTS];
        	int targetBase = typeTrie[targetset+TYPE_COUNTS];
        	int j = typeTrie[subset+TYPE_COUNTS_LEN];
        	while (--j>=0) {
        		
        		
        		int k = 10; //max delta fields
        		while (--k>=0) {
        			
        			valueHistCounts[targetBase+j][k] += valueHistCounts[fromBase+j][k];
        			valueHistCounts[fromBase+j][k] = 0;
        			
        			deltaHistPrevCounts[targetBase+j][k] += deltaHistPrevCounts[fromBase+j][k];
        			deltaHistPrevCounts[fromBase+j][k] = 0;
        			
        			copyHistCounts[targetBase+j][k] += copyHistCounts[fromBase+j][k];
        			copyHistCounts[fromBase+j][k] = 0;
        			
        			incHistCounts[targetBase+j][k] += incHistCounts[fromBase+j][k];
        			incHistCounts[fromBase+j][k] = 0;
        			
        			
        		}
        		        		      
        		
        		
        	}
        	
        	
        	
        }
        
        
        //TODO: A, need to also merge the count totals for operator but these are in another array.
        
        
//        if (subsetHasNull) {
//        	//can only combine with the target if there is only 1 so we know where it will be going.
//        	if (nonNullCount==1) {
//        		//merge null from subset
//        		typeTrie[targetset+nonNullIdx]  |= OPTIONAL_FLAG; 
//        		typeTrie[subset+TYPE_NULL] = 0;
//        	}
//        	//if zero just move it over
//        }
        
        
        return true;
    }
    
    
    private static int lastNonNull(int pos, int[] typeTrie, int startLimit) {
        int i = startLimit;
        while (--i>=0) {
            if (TypeExtractor.TYPE_NULL!=i && TYPE_EOM!=i) {
                int v =OPTIONAL_LOW_MASK&typeTrie[pos+i];
                if (0!=v && notEmpty(v, typeTrie, 0)) {
                    return i;
                }
            }            
        }
        return -1;
    }
    
    private  void recurseCatalog(int pos, final Appendable target, final ItemGenerator[] itemGenerators, final int itemCount, final long[] huffmanCodes) throws IOException {
    	
    	int i = TYPE_STEP_SIZE;
        while (--i>=0) {
        	int offset = pos+i;
            int raw = typeTrie[offset];
            int value = OPTIONAL_LOW_MASK&raw;
            int optionalBit = 1&(raw>>OPTIONAL_SHIFT);

            if (value > 0) {                
            
	            /////
	            //find the id for this field or the entire message
	            //if it was loaded previously use it, if not use the position in the trie
	            //it may also be a hash
	            /////
	            int msgData = typeTrie[pos+TYPE_MIDX];
	            
	            int nameToken = typeTrie[pos+TYPE_NAME_ID];
	            final String name = nameToken!=0 ? lookupName(nameToken) : String.valueOf(offset);
	            
                if (i==TYPE_EOM) {//End of Message
                	recurseCatalogEndOfMessage(pos, target, itemGenerators, itemCount, huffmanCodes, msgData, name);
                	//keep processing this because there may be longer messages
                } 
                	
                	
                if (i<TYPE_TOP_EXCLUSIVE) {
                    
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {        
                    	int operator = OperatorMask.Field_None;
                                                
                        if (i==TypeExtractor.TYPE_NULL) {
                            type = TypeMask.TextUTF8Optional;
                            operator = OperatorMask.Field_Copy;//default case when it is unknown
                        } else {
                            type = type|optionalBit;                            
                        }

                        String initial = null;
                        String initialExp = ""+2;
                               
                        int id;
        	            if ((msgData & HIGH_BIT_MSG_IDX) != 0) {
        	            	id = ID_MASK&msgData;
        	            } else {
        	            	id = offset;
        	            }
        	            
                        if (TypeMask.Decimal==type || TypeMask.DecimalOptional==type ) {
                            //TODO: this uses a constant exponent for all values, we will want to make the settable by field position or name
                            itemGenerators[itemCount] = new FieldGenerator(name,id,1==optionalBit,type,OperatorMask.Field_Copy,OperatorMask.Field_None,initialExp,initial);  
                        } else {      
                            itemGenerators[itemCount] = new FieldGenerator(name,id,1==optionalBit,type,operator,initial);                                      
                        }
                        
                        recurseCatalog(value, target, itemGenerators, itemCount+1, huffmanCodes);    
                    }
                }        
            }
        }        

    }

	protected void recurseCatalogEndOfMessage(int pos, final Appendable target,
			final ItemGenerator[] itemGenerators, final int itemCount,
			final long[] huffmanCodes, int msgData, final String name)
			throws IOException {
		final ItemGenerator[] buf = optimizeOperators(itemGenerators, itemCount, typeTrie[pos+TYPE_COUNTS]);
		
		//was this set as hash
		boolean isHashed = ((msgData & (HIGH_TWO_BC_ID)) == HIGH_TWO_BC_ID);
		
		//does this template have a repeating value that could be a primary key
		boolean isRepeating = couldBeRepeatingKey(bc);
			            
		if (isHashed && isRepeating ) {
			    
				//builds repeating templates for all the hash values
				BloomCountVisitor templateBuilder = new BloomCountVisitor() {
					
					@Override
					public void visit(int hash, int count) {
						
						//Use different dictionary for every message
						//could modify field ids to a similar effect but then reader would need to know new ids
						             
		                    hash = hash+bc.mask;
		                    final int targ = hash;
		                    
		                    final String dictionary='h'+Integer.toHexString(hash);   
		                    		        	               
		                    long code = huffmanCodes[targ];
		                    if (0==code) {
		                    	//   	throw new RuntimeException("Unable to generate templateId for message. No code found for hash "+ targ);
		                    } else {
		                    	huffmanCodes[targ]=0;//wipe out value to track error if this gets hit again.

								try {
									buildTemplate(target, buf, itemCount, code, name, dictionary);
								} catch (IOException e) {
									throw new RuntimeException(e);
								}

		                    }
					}};
						            			
					BloomCounter.visitAbovePercentile(abovePct, templateBuilder, bc);
					//for those skipped a zero will appear in the lookup and the first message position will be used.

			
			//TODO: B, use template Ref from the spec to make the template files much smaller
			//TODO: B, research template, if once set it can be used for following message without setting again.
			
			
		} else { 
			int msgIdx = ID_MASK&msgData;
			
			//roll back to old style because there were no repeating enum values found for huffman

			int templateId = (int)huffmanCodes[msgIdx];
			if (0!=templateId) {
				//put each message type into its own dictionary by default
				String dictionary = "x"+Integer.toHexString(msgIdx);		                    
				buildTemplate(target, buf, itemCount, templateId, name, dictionary);
				//clear to ensure we do not use this one again
				huffmanCodes[msgIdx] = 0;
			} else {
				assert(isHashed) : "unhashed messages must not write mutliple templates";
			}
			
		}
	}
    
    //TODO: B, replace with garbage free implementation
    List<String> names = new ArrayList<String>();
    
    /**
     * Returns token for fetching the name again later.
     * @param name
     * @return
     */
    private int addName(String name) {
    	int value = names.indexOf(name);
    	if (value<0) {
    		value = names.size();
    		names.add(name);
    	}
    	return value+1;
    }

    private String lookupName(int nameToken) {
    	return names.get(nameToken-1);
	}

	/**
     * Returns true if this column contains values that look like enums and may work well as a primary key.
     * To disable the use of this feature this method can be modified to return false in all cases.
     * 
     * @param id
     * @param bloom
     * @return
     */
	private boolean couldBeRepeatingKey(BloomCounter bloom) {
		boolean disable = false;
		if (disable) {
			return false;
		}
		
		BloomCounter.buildSummary(bloom);
		
		int estimatedUniqueKeys = BloomCounter.uniqueCount(bloom);
		if (estimatedUniqueKeys<=0) {
			return false;
		}
		Histogram stats = BloomCounter.stats(bloom);
		long accumTotal = Histogram.accumulatedTotal(stats);
		
		//can only support limited keys due to bloom filter field AND
		//each enum/key must be used on average more than 10 times.  examples from  demo 1101, 736
		return (estimatedUniqueKeys<80000) && ((int)(accumTotal/estimatedUniqueKeys)>10);
	}

	private ItemGenerator[] optimizeOperators(final ItemGenerator[] buffer, final int itemCount, int countIdx) {
		//end of message so go back and find the right operators
		final ItemGenerator[] buf = new ItemGenerator[buffer.length];	
		int k = 0;
		while (k<itemCount) {
			
			
			//TODO: AA, MUST COMPARE THEM ALL AND USE BYTE HISTOGRAM FOR CPY AND INC
			//TODO: AA, MUST AUTO SELECT HASH COLUMN
			
			if (buffer[k] instanceof FieldGenerator) {

				///////////////////////////////
			    //Select the optimal compression operator for this field
		    	///////////////////////////////
				int idx = countIdx + k;
				int oper = OperatorMask.Field_None;
				FieldGenerator fg = (FieldGenerator)buffer[k];
				
				///////
				//compute the bytes needed to store this field using different operators
				///////
				
				int[] valueHist = valueHistCounts[idx];
				int[] deltaPrevHist = deltaHistPrevCounts[idx]; 
				int[] copyHist = copyHistCounts[idx];
				int[] incHist = incHistCounts[idx];
				
				long totalBytesNone = 0;
				long totalBytesDelta = 0;
				long totalBytesCopy = 0;
				long totalBytesInc = 0;
				
				int totalInstanceCount = 0;
				int totalInstanceCopyCount = 0;
				
								
				int j = 10;
				while (--j>0) {
					int temp = valueHist[j];
					totalInstanceCount += temp; 
					totalBytesNone += (j*temp);
					
					totalBytesDelta += (j*deltaPrevHist[j]);

					int copy = copyHist[j];
					totalInstanceCopyCount += copy;
					totalBytesCopy += (j*copy);
					
					totalBytesInc += incHist[j];
					
					
				}
				
				//must inverse both of these to show the bytes that would be consumed not saved
				totalBytesCopy = totalBytesNone - totalBytesCopy;
				totalBytesInc  = totalBytesNone - totalBytesInc;
				
				///////
				///////
							
				
				
				//simple logic for byte arrays and text, TODO: this should be more advanced and check for deltas but this will need a new algo.
				if (fg.isText() || fg.isByteArray()) {
					//estimated that using copy saves 50% or better so use it else we will use delta
					if ( totalInstanceCopyCount >= (totalInstanceCount>>1) ) {
						oper = OperatorMask.Field_Copy;
					} else {
						oper = OperatorMask.Field_Delta;
					}
					//NOTE: 'None' would be nice to save time if we knew the other two always failed but would not save space.
					//NOTE: 'Constant' would be nice if we knew the data was never going to change but copy does just as well on size.
					
				} else {
					//these are the numerics
					
					//pick the smallest none, delta, copy or inc.
					long  totalBytes = totalBytesNone;
					oper = OperatorMask.Field_None;
					
					if (totalBytesCopy<totalBytes) {
						totalBytes = totalBytesCopy;
						oper = OperatorMask.Field_Copy;
					}
					
					if (totalBytesInc<totalBytes) {
						totalBytes = totalBytesInc;
						oper = OperatorMask.Field_Increment;
					}
					
					if (totalBytesDelta<totalBytes) {
						totalBytes = totalBytesDelta;
						oper = OperatorMask.Field_Delta;
					}
					
				}
				
				boolean debug = false;
				if (debug) {				
					System.err.println("pos:"+k+" picked:"+OperatorMask.xmlOperatorName[oper]+" tot:"+totalBytesNone+" cpy:"+totalBytesCopy+" inc:"+totalBytesInc+" delta:"+totalBytesDelta);
				}
			
				
				
				if (fg.isDecimal()) {
					buf[k] = fg.clone(OperatorMask.Field_Copy, oper);					
				} else {
					buf[k] = fg.clone(oper);
				}
				
			} else {
				buf[k] = buffer[k];
			}
			k++;
		}
		return buf;
	}

	private void buildTemplate(Appendable target, ItemGenerator[] buffer,
								int itemCount, long id, String name, String dictionary) throws IOException {
		boolean reset=false;
		TemplateGenerator.openTemplate(target, name, id, reset, dictionary);                    
		int j = 0;
		while (j<itemCount) {
		    buffer[j].appendTo("    ", target);
		    j++;
		}                        
		TemplateGenerator.closeTemplate(target);
	}
    
	
	
    private  void recurseTemplates(int pos, final BloomCountVisitor huffmanBuilder) throws IOException {
    	   	
    	
        int i = TYPE_TOP_EXCLUSIVE;
        while (--i>=0) {
        	int offset = pos+i;
            int value = OPTIONAL_LOW_MASK&typeTrie[offset];
            
            if (value > 0) {              	
            	recurseTemplates(value, huffmanBuilder);      
            }
        }   
        
        int eomValue = OPTIONAL_LOW_MASK&typeTrie[pos+TYPE_EOM];
        
        if (eomValue > 0) {
        
        	//todo: NEED COMMON FUNCTION TO GET THE ID

            int possibleId = typeTrie[pos+TYPE_MIDX];                      
            
            if (((possibleId & HIGH_TWO_BC_ID) == HIGH_TWO_BC_ID) ) {
            	//this is a hash

        		BloomCountVisitor huffmanBuilder2 = new BloomCountVisitor() {

					@Override
					public void visit(int hash, int count) {
						huffmanBuilder.visit(hash+bc.mask,count);
					}
					
        		};
				BloomCounter.visitAbovePercentile(abovePct, huffmanBuilder2, bc);

				//huffman code will be required for all values even if the bloom counter is used.
				//this is because some hashed values do not happen frequently and will need a huffman code
				huffmanBuilder.visit(ID_MASK&possibleId, 1); //frequency is set very low for this rare case
            } else {
            	//normal case
            	huffmanBuilder.visit(ID_MASK&possibleId, typeTrie[pos+TYPE_EOM]);
            }
         }

    }
	
    
    public static Appendable buildCatalog(RecordFieldExtractor rfe, boolean withCatalogSupport, Appendable target) throws IOException {       
            	    	
    	final long[] templateIdArray = buildHuffmanTemplateIds(rfe);    	
    	
        target.append(CatalogGenerator.HEADER);
               
        //The generators are build here once a "working space" for building each message
        int generators = 2 + (Math.max(rfe.maxFieldCount,16) * 2);
        rfe.recurseCatalog(0, target, new ItemGenerator[generators], 0, templateIdArray);
                
        if (withCatalogSupport) {
        	int lastSafeIdx = rfe.typeTrieLimit + TYPE_STEP_SIZE;
        	rfe.addTemplateToHoldTemplates(target, lastSafeIdx);
        }
                
        target.append(CatalogGenerator.FOOTER);
        
        //TODO: T, the number of templates in the catalog must be >= to the number of messages in the TRIE
        
        return target;        
    }

	public static long[] buildHuffmanTemplateIds(RecordFieldExtractor rfe)
			throws IOException {
		//Build template IDs based on huffman code so we the smallest ones possible for the greatest number of records.
		final Huffman huff = new Huffman();

		BloomCountVisitor huffmanBuilder = new BloomCountVisitor() {	            				
			@Override
			public void visit(int hash, int freq) {
				Huffman.add(hash, freq, huff);				
			}
		};		
		
		rfe.recurseTemplates(0, huffmanBuilder);		
		
		HuffmanTree tree = Huffman.encode(huff);  	
    	
		//build array for quick lookup.
    	final long[] huffmanCodes = new long[TokenBuilder.MAX_FIELD_ID_VALUE];
    	tree.visitCodes(new HuffmanVisitor(){
			@Override
			public void visit(int value, int frequency, long code) {
				assert(0!=code);
				huffmanCodes[value] = code;
			}}, 1l);
		return huffmanCodes;
	}

    public void addTemplateToHoldTemplates(Appendable target, int catTempId) throws IOException {
        String name="catalog";
        int id=CATALOG_TEMPLATE_ID;
        
        boolean reset=false;
        String dictionary="global";

        TemplateGenerator.openTemplate(target, name, id, reset, dictionary);
                
        boolean presence = false;
        int operator = OperatorMask.Field_None;
        String initial = null;
        int type = TypeMask.ByteArray;
        
        FieldGenerator fg;
        fg = new FieldGenerator(Integer.toString(catTempId),catTempId,presence,type,operator,initial);  
        fg.appendTo("    ", target);            
        
        TemplateGenerator.closeTemplate(target);
    }
    
    public void newCatBytesImpl() {
    	
    	//on 2 Gig test file
    	// 1. encoding takes    100 seconds
    	// 2. schema extraction  65 seconds
    	// 3. SAX parse takes    24 seconds
    	
    	//instead of using SAX paser convert directly from this strucutre into this save method!!
    	
//        // write catalog data.
//        TemplateCatalogConfig.save(writer, fieldIdBiggest, templateIdUnique, templateIdBiggest, defaultConstValues,
//                catalogLargestTemplatePMap, catalogLargestNonTemplatePMap, tokenIdxMembers, tokenIdxMemberHeads,
//                catalogScriptTokens, catalogScriptFieldIds, catalogScriptFieldNames, 
//                catalogTemplateScriptIdx, templateIdx, templateLimit,
//                maxGroupTokenStackDepth + 1, clientConfig);
//
//        // close stream.
//        PrimitiveWriter.flush(writer);
        
        
    }
    
    
    public byte[] catBytes(ClientConfig clientConfig) {
        String catalog;
		try {
			//TODO: A, replace with inputstream that is an appendable
			catalog = buildCatalog(this, true, new StringBuilder()).toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
        
        boolean debug = false;
        if (debug) {
        	System.err.println("catalog:\n"+catalog+"\n");
        }
        clientConfig.setCatalogTemplateId(CATALOG_TEMPLATE_ID);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gZipOutputStream = new GZIPOutputStream(baos);
            FASTOutput output = new FASTOutputStream(gZipOutputStream);
            
            SAXParserFactory spfac = SAXParserFactory.newInstance();
            SAXParser sp = spfac.newSAXParser();
            
            
            InputStream stream = new ByteArrayInputStream(catalog.getBytes(StandardCharsets.UTF_8));           
            
            TemplateHandler handler = new TemplateHandler();            
            sp.parse(stream, handler);    
           
    		PrimitiveWriter writer = new PrimitiveWriter(4096, output, false);
    		//TODO: A, need to get the biggest length seen in the input data.
    		TemplateCatalogConfig.writeTemplateCatalog(handler, 16, 512, writer, clientConfig);
    		
            gZipOutputStream.close();            
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        byte[] catBytes = baos.toByteArray();
        return catBytes;
    }

    public byte[] memoizeCatBytes() {
         return catBytes = catBytes(new ClientConfig(20,26)); //TODO: A, expose these constants!        
    }
    
    public byte[] getCatBytes() {
        return catBytes;
    }


    /**
     * Load catalog from this file and use it
     * @param templateFile
     */
	public void loadTemplate(String source, ClientConfig clientConfig, TypeExtractor typeExtractor) {
		
        catBytes = buildCatBytes(source, clientConfig);
        
        //load all the fields to be used by the parser
        TemplateCatalogConfig config = new TemplateCatalogConfig(catBytes);        
        loadFROM(config.getFROM());
	}

	public void loadTemplate(byte[] catBytes, TypeExtractor typeExtractor) {
		this.catBytes = catBytes;
		loadFROM(new TemplateCatalogConfig(catBytes).getFROM());
	}
	
	public void loadTemplate(byte[] catBytes, TemplateCatalogConfig config, TypeExtractor typeExtractor) {
		this.catBytes = catBytes;
		loadFROM(config.getFROM());
	}
	
	public void loadFROM(FieldReferenceOffsetManager from) {
		
		//TODO: AA, for very large templates this does not appear to load everything.
		
		int[] entries = from.messageStarts;
        int[] scripts = from.tokens;
        String[] dictionaryScript = from.dictionaryNameScript;  
        String[] fieldNameScript = from.fieldNameScript;
        
   //     System.err.println("loaded dictionary values:"+Arrays.toString(dictionaryScript));
        
        int j = entries.length;
        System.err.println("loading catalog with "+j+" entries");
       
        String lastName = null;
        while (--j>=0) {
			resetToRecordStart(this);
        	int i = entries[j];
        	String name = fieldNameScript[i];
        	
    		//if the last message definition was the same as this one do not bother reloading it.
    		//this happens for the bloom filters.
    		if (null==name || !name.equals(lastName)) {
    			lastName = name;
    			System.err.println("loading message of name:"+name);
    			
	    		int token;
	        	do {
	        		name = fieldNameScript[i];
	        		token = scripts[i++];    
	        		        		
	        		int type = TokenBuilder.extractType(token); 
	        		
	        		int simpleType = type>>1;
	                //jumps over group def and other non type tokens    
	        		if (simpleType<TypeExtractor.TYPE_NULL) {        			
		        		
		        		appendNewField(((type&1)<<OPTIONAL_SHIFT) | simpleType, name , (type&1) == 0 ? 0: OPTIONAL_FLAG);  
		        		
		        		if (TypeExtractor.TYPE_DECIMAL == simpleType) {
		        			//special case because a decimal takes up two slots, TODO: change to data driven impl?
		        			i++;	        			
		        		}	        			        		
	        		} 
	
	        	} while (TokenBuilder.extractType(token)!=TypeMask.Group || 
	        			 0==(TokenBuilder.extractOper(token)&OperatorMask.Group_Bit_Close)  );
	        	
    		}

        	int msgIdx = entries[j];
            int mask;
        	
        	String dictionary = dictionaryScript[msgIdx];
        	int hash = extractHash(dictionary); 
        	if (hash >= 0) {
        		mask = HIGH_TWO_BC_ID;
        		//a value zero is never used so that we can tell if its undefined later
        		validMessageIdx[hash] = msgIdx+1; //inc by 1 here to prevent using zero
        	} else {
        		mask = HIGH_BIT_MSG_IDX;
        	}
        	//always holds msgIdx for the case when we do not hash and for the case when we do hash but it has nothing to map the value
        	typeTrie[typeTrieCursor+TYPE_MIDX] = mask | (int)msgIdx;
    
        	//add 1 to this message count in order to validate it as a real message vs an artifact left from parsing
        	typeTrie[typeTrieCursor+TYPE_EOM]++;
        }
		
        boolean debug = false;
        if (debug) {
	        try {
				System.err.println("loaded data:\n"+buildCatalog(this, false, new StringBuilder()).toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
	        printRecursiveReport(0, " ");
	        System.err.println();

        }
        
        //now loaded so print report of what we now have
        System.err.println("LOADED FROM ");
        printRecursiveReport(0,"  ");
        
	}

	

	private int extractHash(String dictionary) {
		if (null!=dictionary && dictionary.length()>1 && dictionary.charAt(0)=='h') {
			try{
				return Integer.parseInt(dictionary.substring(1), 16);
			} catch (Exception e) {
				//ignore, this is just not a hash code, not a big deal.
			}
		}
		return -1;
	}

	public static byte[] buildCatBytes(String source, ClientConfig clientConfig) {
		ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, source, clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert(catalogBuffer.size() > 0);
        return catalogBuffer.toByteArray();
	}

	public static byte[] buildCatBytes(InputStream source, ClientConfig clientConfig) {
		ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, source, clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert(catalogBuffer.size() > 0);
        return catalogBuffer.toByteArray();
	}


    
}
