package com.ociweb.pronghorn.components.ingestion.dynamic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.jfast.FAST;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.components.ingestion.csv.FieldSplitterStage;
import com.ociweb.pronghorn.components.ingestion.csv.LineSplitterByteBufferStage;
import com.ociweb.pronghorn.components.ingestion.csv.LineSplitterFileChannelStage;
import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.CatByteConstantProvider;
import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.FASTDecodeStage;
import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.FASTEncodeStage;
import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.RecordFieldExtractor;
import com.ociweb.pronghorn.components.ingestion.dynamic.extraction.TypeExtractor;
import com.ociweb.pronghorn.components.ingestion.dynamic.stage.MessageGeneratorStage;
import com.ociweb.pronghorn.components.ingestion.dynamic.stage.TemplateGeneratorStage;
import com.ociweb.pronghorn.components.ingestion.file.FileWriteStage;
import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;

/**
 * Command line application for importing and exporting data
 * between comma delimited and FAST encoded formated.
 * 
 * @author Nathan Tippy
 *
 */
public class IngestUtil {  

	private static final String ARG_TEMPLATE = "template";
	private static final String ARG_FAST = "fast";
	private static final String ARG_CSV = "csv";
	private static final String ARG_TASK = "task";
	
	private static final String VALUE_PIPE = "pipe";
	
	private static final String VALUE_INGEST = "ingest";
	private static final String VALUE_ENCODE = "encode";
	private static final String VALUE_DECODE = "decode";
    private static Logger log = LoggerFactory.getLogger(IngestUtil.class);
	
	private static RingBufferConfig linesRingConfig = new RingBufferConfig((byte)12,(byte)21,null, FieldReferenceOffsetManager.RAW_BYTES);
	private static RingBufferConfig fieldsRingConfig = new RingBufferConfig((byte)10,(byte)19,null,  MetaMessageDefs.FROM);
	private static RingBufferConfig flatFileRingConfig = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 1000, 4096);
	
	
    public static void main(String[] args) {        
    	
    	String templateFilePath = getValue(ARG_TEMPLATE,args,null);
    	String fastFilePath = getValue(ARG_FAST,args,null);
    	String csvFilePath = getValue(ARG_CSV,args,null);
    	String task = getValue(ARG_TASK,args,null);//encode or decode
    	
    	//validate that we have all the needed args
    	
    	if (null == templateFilePath ||
    		null == fastFilePath ||
    		null == csvFilePath ||
    		null == task) {
    		    		
    		
    		System.err.println(IngestUtil.class.getSimpleName()+" -template <templateFile> -fast <fastFile> -csv <csvFile> -task encode|decode|ingest");
    		System.exit(-1);
    		return;
    	}
    	
    	//NOTE:
    	//for all writes it will append or create if it is missing, never delete any files
    	
    	//task specific validation 
    	
    	    	
    	long totalMessages = 0;
    	long totalBytes = 0;
    	
    	
    	
    	if (VALUE_INGEST.equalsIgnoreCase(task) || VALUE_PIPE.equalsIgnoreCase(task)) {
    		log.info("starting ingest");
    		long startTime = System.currentTimeMillis();
    		
    		File csvFile = new File(csvFilePath);
    		if (!csvFile.exists()) {
    			System.err.println("csv file not found: "+csvFilePath);
    			System.exit(-1);
    			return;
    		}
    		totalBytes = csvFile.length();
    		
    		File templateFile = new File(templateFilePath);
    		
    		TypeExtractor typeExtractor = new TypeExtractor(true /* force ASCII */);
    		RecordFieldExtractor typeAccum = new RecordFieldExtractor();   
   			totalMessages = IngestUtil.encodeAndBuildTemplate(csvFile, templateFile, typeAccum, new File(fastFilePath), typeExtractor); 

   	    	long duration = System.currentTimeMillis()-startTime;
   	    	
   	    	float msgPerMS = ((float)totalMessages/(float)duration);
   	    	
   	    	long bitsPerMS = (totalBytes*8)/duration;
   	    	    	
   	    	System.out.println("runtime "+duration+" ms "+totalMessages+" messages "+msgPerMS+" msgPerMS "+bitsPerMS+" bitsPerMS ");
   	    	System.out.println();
    	}
    	
    	if (VALUE_ENCODE.equalsIgnoreCase(task) || VALUE_PIPE.equalsIgnoreCase(task)) {
    		log.info("starting encode");
    		long startTime = System.currentTimeMillis();
    		
    		File csvFile = new File(csvFilePath);
    		if (!csvFile.exists()) {
    			System.err.println("csv file not found: "+csvFilePath);
    			System.exit(-1);
    			return;
    		}
    		totalBytes = csvFile.length();
    		    		
    		TypeExtractor typeExtractor = new TypeExtractor(true /* force ASCII */);
    		RecordFieldExtractor typeAccum = new RecordFieldExtractor();   

    		totalMessages = IngestUtil.encodeGivenTemplate(csvFile, templateFilePath, typeAccum, fastFilePath, typeExtractor);  
	   		
	   		File resultFile = new File(fastFilePath);
	   		long resultSize = resultFile.length();
	   				
	   		float pct = 100*(1f-(resultSize/(float)csvFile.length()));
	   		System.out.println("total bytes in compressed output:"+resultSize+" raw input:"+csvFile.length()+"  Compt: "+pct+"%");
	   		
   	    	long duration = System.currentTimeMillis()-startTime;
   	    	
   	    	float msgPerMS = ((float)totalMessages/(float)duration);
   	    	
   	    	long bitsPerMS = (totalBytes*8)/duration;
   	    	    	
   	    	System.out.println("runtime "+duration+" ms "+totalMessages+" messages "+msgPerMS+" msgPerMS "+bitsPerMS+" bitsPerMS ");

    	}
    	
    	//TODO: NOTE: it is very very easy to use the wrong template file with the binary.
    	//            1. we can build a test framework that reads from one directly into the next
    	//            2. we can build a build in signature check in jFAST.
    	
    	
//    	//TOOD: decode is not completed, revisit after the threading model is changed.
//    	if (VALUE_DECODE.equalsIgnoreCase(task) || VALUE_PIPE.equalsIgnoreCase(task)) {
//    		long startTime = System.currentTimeMillis();
//    		
//    		File templateFile = new File(templateFilePath);
//    		if (!templateFile.exists()) {
//    			System.err.println("template file not found: "+templateFilePath);
//    			System.exit(-1);
//    			return;
//    		}
//    		File fastFile = new File(fastFilePath);
//    		if (!fastFile.exists()) {
//    			System.err.println("fast file not found: "+fastFilePath);
//    			System.exit(-1);
//    			return;
//    		}
//    		totalBytes = fastFile.length();
//    		
//    		totalMessages = IngestUtil.decode(templateFilePath,fastFile);
//    		
//    		long duration = System.currentTimeMillis()-startTime;
//   	    	
//   	    	float msgPerMS = ((float)totalMessages/(float)duration);
//   	    	
//   	    	long bitsPerMS = (totalBytes*8)/duration;
//   	    	    	
//   	    	System.out.println("runtime "+duration+" ms "+totalMessages+" messages "+msgPerMS+" msgPerMS "+bitsPerMS+" bitsPerMS ");
//    	} 
    	

    	
    	
    }
    
    
    
    private static int decode(String templatePath, File fastFile) {
    	
    	//TODO: convert to real stages
    	// Stage 1 read fast file into byte stream
    	// Stage 2 convert fast bytes into messages
    	// Stage 3 read messages into ASCII stream
    	// stage 4 write ASCII stream to new CSV
    	
    	
    	System.err.println(fastFile);
    	System.err.println(templatePath);
    	ClientConfig clientConfig = new ClientConfig();  //TODO: need to know how big to make this!!
    	
    	if (templatePath.contains("complex")) {
    		System.err.println("WARNING: added preamble for complex example.");
    		clientConfig.setPreableBytes((short)4); //only needed for complex file
    	}
    	
		byte[] catBytes = RecordFieldExtractor.buildCatBytes(templatePath, clientConfig);

		
		
		 TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
		 FieldReferenceOffsetManager customFrom = catalog.getFROM();
		 
		 RingBufferConfig fastBytesRingConfig = new RingBufferConfig((byte)4,(byte)20,null, FieldReferenceOffsetManager.RAW_BYTES);
		 RingBufferConfig customRingConfig = new RingBufferConfig((byte)16,(byte)25,null, customFrom);
		 RingBufferConfig csvFileRingConfig = new RingBufferConfig((byte)16,(byte)25,null, FieldReferenceOffsetManager.RAW_BYTES);
		 		
		 RingBuffer fastBytesRing = new RingBuffer(fastBytesRingConfig);
		 RingBuffer customRing = new RingBuffer(customRingConfig);
		 RingBuffer csvFileRing = new RingBuffer(csvFileRingConfig);
		 
		 GraphManager gm = new GraphManager();
		 
//		 Runnable fileLoader = new FileLoaderStage(fastFile, fastBtytesRing);
		 PronghornStage fastDecoder = new FASTDecodeStage(gm, fastBytesRing, new CatByteConstantProvider(catBytes), customRing);
//		 Runnable csvFeed = new CSVProducerStage(customRing, csvFileRing);
		 PronghornStage fileSave = new FileWriteStage(gm, csvFileRing,new File(fastFile+".csv"));
		 
	//	 executeInParallel(fileLoader, fastDecoder, csvFeed, fileSave);
		 
		 
        
        System.err.println(fastFile.getName()+ " "+fastFile.length());
        
        
        
        FASTInput fastInput = null;
		try {
//			FileInputStream fist1 = new FileInputStream(fastFile);
//			int j = 30;
//			while (--j>=0) {
//				int r = fist1.read();
//				System.out.println(r+"  "+Integer.toBinaryString(r)+"  "+(r&0x7F));
//			}
//			
//			fist1.close();
			
			FileInputStream fist = new FileInputStream(fastFile);
			fastInput = new FASTInputStream(fist);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
    	
		final AtomicInteger msgs = new AtomicInteger();		
		FASTClassLoader.deleteFiles();

        RingBuffers buildRingBuffers = TemplateCatalogConfig.buildRingBuffers(new TemplateCatalogConfig(catBytes), (byte)10, (byte)24);
		FASTReaderReactor reactor = FAST.inputReactorDebug(65536, fastInput, catBytes, buildRingBuffers); 

        int or=0;
        int yr=0;
        int mo=0;
        int dy=0;
        long vol = 0;
        byte[]  sym = new byte[32];
        byte[]  comp = new byte[48];
        
        int symLen = 0;
        int compLen = 0;
        
        
        try{
        	//Takes binary stream from the reader and puts messages on the ring buffers
        while (FASTReaderReactor.pump(reactor)>=0) { 
        	int k = reactor.ringBuffers().length;
        	while (--k>=0) {
        	    RingBuffer rb = reactor.ringBuffers()[k];
        	    
        	    //if this ring buffer has a message consume it
        		if (RingReader.tryReadFragment(rb)) {
	        		if (RingReader.isNewMessage(rb)) {
	        			int fragStart = RingReader.getMsgIdx(rb);
	        			FieldReferenceOffsetManager from = RingBuffer.from(rb);
	        				        			
						//System.err.println("msg Idx:"+fragStart+" "+from.fragDataSize[fragStart]+"  "+from.fieldNameScript[fragStart]+"  "+  TokenBuilder.tokenToString(from.tokens[fragStart]));
	        			
						StringBuilder builder = new StringBuilder();
						int fieldCount = from.fragScriptSize[fragStart];
						int i = 1;
						while (i<fieldCount) {
							builder.setLength(0);
							int j = fragStart+i++;
							builder.append("  "+TokenBuilder.tokenToString(from.tokens[j])+"  '"+from.fieldNameScript[j]+"'" );
							//using the name look up the field locator
							int fieldLoc = FieldReferenceOffsetManager.lookupFieldLocator(from.fieldNameScript[j],fragStart,RingBuffer.from(rb));
							
							switch (TokenBuilder.extractType(from.tokens[j])) {
								case TypeMask.IntegerSigned:
								case TypeMask.IntegerUnsigned:
								case TypeMask.IntegerSignedOptional:
								case TypeMask.IntegerUnsignedOptional:
									builder.append("  int:"+RingReader.readInt(rb, fieldLoc));
								break;
								case TypeMask.LongSigned:
								case TypeMask.LongUnsigned:
								case TypeMask.LongSignedOptional:
								case TypeMask.LongUnsignedOptional:
									builder.append("  long:"+RingReader.readLong(rb, fieldLoc));
								break;
								case TypeMask.Decimal:
								case TypeMask.DecimalOptional:
									
									int exp = RingReader.readDecimalExponent(rb, fieldLoc);
									long mant = RingReader.readDecimalMantissa(rb, fieldLoc);
																		
									double decimald = RingReader.readDouble(rb, fieldLoc);
									float decimalf = RingReader.readFloat(rb, fieldLoc);
									
									builder.append("  decimalD:"+decimald+"  decimalF:"+decimalf+"  decimal ex:"+exp+"  decimal mant:"+mant);
									i++;//add 1 extra because decimal takes up 2 slots in the script
								break;	
								case TypeMask.TextASCII:
								case TypeMask.TextASCIIOptional:
									int lenASCII = RingReader.readBytesLength(rb, fieldLoc);
									
									builder.append(" len:"+lenASCII+"  ASCII:"+RingReader.readASCII(rb, fieldLoc, new StringBuilder()));
								break;
								case TypeMask.TextUTF8:
								case TypeMask.TextUTF8Optional:
									int lenUTF8 = RingReader.readBytesLength(rb, fieldLoc);
									int posUFT8 = RingReader.readBytesPosition(rb, fieldLoc);
									
									builder.append(" pos:"+posUFT8+" len:"+lenUTF8+"  "+Integer.toBinaryString(lenUTF8));//+"  UTF8:"+RingReader.readUTF8(rb, fieldLoc, new StringBuilder()));
								break;
							}
							System.err.println(builder);
						}
			
	        			msgs.incrementAndGet();
	        		}
        		}
        		
        	}
        	
        	
        }
        } finally {
        	System.out.println("total messages:"+msgs+" last "+or+","+yr+","+mo+","+dy+"   vol:"+vol+"   "+new String(sym,0,symLen)+" "+new String(comp,0,compLen));
        	RingBuffer rb = reactor.ringBuffers()[0];
        	System.err.println(rb);
       
        }
		return msgs.intValue();
	}

	//TODO: A, build out this utility to be static only given template files

    //debug tips, 
    //            use a very large ring to keep all the messages leadin up
    //            use a very small ring to lock one stage with the previous.

	private static long encodeGivenTemplate(File csvFile, String templateFile,
											RecordFieldExtractor typeAccum, String fastFilePath, TypeExtractor typeExtractor) {
		
				 		 
		 //loading and parsing the file to ensure it was good
		 byte[] catBytes = TemplateLoader.buildCatBytes(templateFile,  new ClientConfig());
		 TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
		 FieldReferenceOffsetManager customFrom = catalog.getFROM();					
		
		 long results = 0;
		 
		 try {

			 RingBufferConfig customRingConfig = new RingBufferConfig(customFrom, 20, 64);
			 
			 //input test data		
			FileChannel fileChannel = new RandomAccessFile(csvFile, "r").getChannel();     
				
			//build all the ring buffers
			GraphManager gm = new GraphManager();
			
			//build all the stages
			LineSplitterByteBufferStage lineSplitter =  new LineSplitterFileChannelStage(gm,fileChannel,new RingBuffer(linesRingConfig));
			FieldSplitterStage fieldSplitter = new FieldSplitterStage(gm,GraphManager.getOutputPipe(gm, lineSplitter, 1), new RingBuffer(fieldsRingConfig));			
			MessageGeneratorStage generatorStage = new MessageGeneratorStage(gm, GraphManager.getOutputPipe(gm, fieldSplitter, 1), new RingBuffer(customRingConfig));
			
			//ConsoleStage cs = new ConsoleStage(gm,GraphManager.getOutputRing(gm, generatorStage, 1));
			
			//ConsoleDataDumpStage ex = new ConsoleDataDumpStage(gm, GraphManager.getOutputRing(gm, generatorStage, 1),42);
			
			FASTEncodeStage encodeStage = new FASTEncodeStage(gm, GraphManager.getOutputPipe(gm, generatorStage, 1), new CatByteConstantProvider(catBytes), new RingBuffer(flatFileRingConfig));
			FileWriteStage fileWriter = new FileWriteStage(gm, GraphManager.getOutputPipe(gm, encodeStage, 1),new File(fastFilePath));
			
			//put the stages into the executors
			executeInParallel(gm,false);
			
			
			
			
			results = generatorStage.getMessages();
		} catch (IOException e) {
		    System.err.println(e.getLocalizedMessage());
		    System.exit(-2);
		    return -1;
		}

		 
		return results;			
		
	}

	public static void executeInParallel(GraphManager gm, boolean batch) {
				
		MonitorConsoleStage.attach(gm);
		if (batch) {
			GraphManager.enableBatching(gm);
		}
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		 
		scheduler.startup();
				
		//TODO: AAA, need to start timer only after some stage has started shutdown (blocking call in graph?)
        boolean cleanExit = scheduler.awaitTermination(120, TimeUnit.MINUTES);
        if (!cleanExit) {
        	log.error("did not shut down clean, see error log");
		}
		
	}
	
	public static void executeInDaemon(Runnable ... run) {
		
		int i = run.length;
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(i, new ThreadFactory(){
		
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread();
			t.setDaemon(true);
			return t;
		}});
		while (--i>=0) {
			executor.scheduleAtFixedRate(run[i],0,40,TimeUnit.MILLISECONDS);
		}
		
	}

 
	 
	private static long encodeAndBuildTemplate(File csvFile,
										File templateFile, RecordFieldExtractor typeAccum, File fastFile, TypeExtractor typeExtractor) {
		
				 long results = 0;
				 boolean testFileCopyOnly = false; //TODO: turn on when we want to start profiling of just the IO portiuon.
				 
				 try {										
						
					//build all the ring buffers
					RingBuffer linesRing = new RingBuffer(linesRingConfig);
					RingBuffer fieldsRing = new RingBuffer(fieldsRingConfig);
					RingBuffer flatFileRing = new RingBuffer(flatFileRingConfig);				
			
										
					GraphManager gm = new GraphManager();
					
					//input test data		
					FileChannel fileChannel = new RandomAccessFile(csvFile, "r").getChannel();     
					LineSplitterByteBufferStage lineSplitter = new LineSplitterFileChannelStage(gm, fileChannel, linesRing);
					
					if (testFileCopyOnly) {
						System.err.println("testing copy only");
						
						ToOutputStreamStage writer = new ToOutputStreamStage(gm, linesRing, new FileOutputStream(templateFile), true);
	
						executeInParallel(gm, true);
						System.err.println("wrote out to: "+templateFile);
						System.exit(0);
					} else {
	
						FieldSplitterStage fieldSplitter = new FieldSplitterStage(gm, linesRing, fieldsRing);
						TemplateGeneratorStage generatorSplitter = new TemplateGeneratorStage(gm, fieldsRing, flatFileRing);
						ToOutputStreamStage writer = new ToOutputStreamStage(gm, flatFileRing, new FileOutputStream(templateFile), false);
							
						//put the stages into the executors
						executeInParallel(gm, true);
					}

					
					results = lineSplitter.getRecordCount();
				} catch (IOException e) {
				    System.err.println(e.getLocalizedMessage());
				    System.exit(-2);
				    return -1;
				}
			 
				 System.err.println("FILE:"+templateFile.getAbsolutePath());
				 
				 if (!testFileCopyOnly) {
					 //loading and parsing the file to ensure it was good
					 byte[] catBytes = TemplateLoader.buildCatBytes(templateFile.getAbsolutePath(),  new ClientConfig());
					 TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
					 FieldReferenceOffsetManager from = catalog.getFROM();

				 }
				 
				 return results;
		
	}


    
    /**
     * pull from command line and if its not found there then pull the system value.
     * if neither of these are found the default is returned.
     * @param key
     * @param args
     * @return
     */
    public static String getValue(String key, String[] args, String def) {
        boolean found = false;
        for(String arg:args) {
            if (found) {
                return arg;
            }
            found = arg.toUpperCase().endsWith(key.toUpperCase());
        }
        return System.getProperty(key,def);
    }
    
//	private static Runnable dumpStage(final RingBuffer inputRing,final AtomicLong count) {
//		
//		return new Runnable() {
//
//			
//            @Override
//            public void run() {           	
//    	           	
//            	           	
//                    long messageCount = 0;
//                    long bytes =0;
//                    while (true) {
//                        
//                    	int iter = RingBuffer.contentRemaining(inputRing)>>1;
//	    				while (--iter>=0) {
//	                    	int meta = takeRingByteMetaData(inputRing);//side effect, this moves the pointer.
//	                    	int len = takeRingByteLen(inputRing);
//	                    	
//	                    	//doing nothing with the data
//	                    	releaseReadLock(inputRing);
//	                    	
//	
//	                    	if (len<0) {
//	                    		count.set(bytes);
//	                    		System.out.println("exited after reading: Msg:" + messageCount+" Bytes:"+count);
//	                    		return;
//	                    	} 
//	                    	bytes+=len;
//	                    	messageCount++;
//	                    	
//                    	} 
//                        Thread.yield();
//                    }   
//                    
//            }                
//        };
//	}
    
    
}