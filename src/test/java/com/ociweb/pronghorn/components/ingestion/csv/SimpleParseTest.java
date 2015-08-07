package com.ociweb.pronghorn.components.ingestion.csv;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.pronghorn.components.ingestion.dynamic.stage.TemplateGeneratorStage;
import com.ociweb.pronghorn.components.ingestion.file.FileWriteStage;
import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.stream.ByteVisitor;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;

public class SimpleParseTest {

	public static final boolean fileSpecificTestValues = true;
	private static final String TEST_FILE = "/roundtrip/small.csv";
	//private static final String TEST_FILE = "/home/nate/flat/salesDemoFileSmall.csv"; ///do not check this in.
	
	
	static ByteBuffer sourceBuffer;
	static RingBufferConfig linesRingConfig;
	static RingBufferConfig fieldsRingConfig;
	static RingBufferConfig flatFileRingConfig;
	
	final int MSG_DOUBLE_LOC = lookupTemplateLocator("Decimal",  MetaMessageDefs.FROM);  
	final int DOUBLE_VALUE_LOC = lookupFieldLocator("Value", MSG_DOUBLE_LOC,   MetaMessageDefs.FROM);
	
	final int MSG_UINT32_LOC = lookupTemplateLocator("UInt32",  MetaMessageDefs.FROM);  
	final int UINT32_VALUE_LOC = lookupFieldLocator("Value", MSG_UINT32_LOC,   MetaMessageDefs.FROM);
	
	final int MSG_UINT64_LOC = lookupTemplateLocator("UInt64",  MetaMessageDefs.FROM);  
	final int UINT64_VALUE_LOC = lookupFieldLocator("Value", MSG_UINT64_LOC,   MetaMessageDefs.FROM);
	
	final int MSG_INT32_LOC = lookupTemplateLocator("Int32",  MetaMessageDefs.FROM);  
	final int INT32_VALUE_LOC = lookupFieldLocator("Value", MSG_INT32_LOC,   MetaMessageDefs.FROM);
	
	final int MSG_INT64_LOC = lookupTemplateLocator("Int64",  MetaMessageDefs.FROM);  
	final int INT64_VALUE_LOC = lookupFieldLocator("Value", MSG_INT64_LOC,   MetaMessageDefs.FROM);
	
	final int MSG_ASCII_LOC = lookupTemplateLocator("ASCII",  MetaMessageDefs.FROM);  
	final int ASCII_VALUE_LOC = lookupFieldLocator("Value", MSG_ASCII_LOC,   MetaMessageDefs.FROM);
	
	@BeforeClass
	public static void setup() {
		
		InputStream testStream = SimpleParseTest.class.getResourceAsStream(TEST_FILE);
		if (null==testStream) {
			try {
				testStream = new FileInputStream(TEST_FILE);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				fail();
			}
		}
				
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
	    try {
	    	byte[] buffer = new byte[1024];
	    	int len = 0;	    	
			while((len = testStream.read(buffer)) != -1){
				// assemble to the a buffer 
				baos.write(buffer, 0, len);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    sourceBuffer = ByteBuffer.wrap(baos.toByteArray());
				
	    linesRingConfig = new RingBufferConfig((byte)20,(byte)26,null, FieldReferenceOffsetManager.RAW_BYTES);
	    fieldsRingConfig = new RingBufferConfig((byte)20,(byte)28,null,  MetaMessageDefs.FROM);
	    flatFileRingConfig = new RingBufferConfig((byte)20,(byte)28,null, FieldReferenceOffsetManager.RAW_BYTES);
	    
	}
	
	
	@Test
	public void testTestData() {
		 
		assertNotNull(sourceBuffer);
		assertTrue(sourceBuffer.remaining()>0);
		System.out.println("TestFile size:"+sourceBuffer.remaining());
	}
	
	@Test
	public void testLineReader() {
		ByteBuffer data = sourceBuffer.asReadOnlyBuffer();				
		RingBuffer linesRing = new RingBuffer(linesRingConfig);
		linesRing.initBuffers();
		
		//start near the end to force the rollover to happen.
		long start = linesRing.sizeOfStructuredLayoutRingBuffer-15;
		int startBytes = linesRing.sizeOfUntructuredLayoutRingBuffer-15;
		linesRing.reset((int)start,startBytes);			
		
		GraphManager gm = new GraphManager();
		
		LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		lineSplitter.startup();
		lineSplitter.blockingRun();
		lineSplitter.shutdown();
		
		RingStreams.writeEOF(linesRing); //HACK to be fixed later
		RingStreams.visitBytes(linesRing, buildLineTestingVisitor());
		
	}
	
	@Test
	public void testLineReaderRollover() {
		//Tests many different primary ring sizes to force rollover at different points.
		//Checks that every run produces the same results as the previous run.
				
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();

		byte[] last = null;
		
		ByteBuffer data = generateCVSData(21);		    
		int dataSize = data.limit();
		
		
		int t = 10;
		while (--t>=4) {
			data.position(0);
			data.limit(dataSize);
			GraphManager gm = new GraphManager();
			
			RingBufferConfig linesRingConfigLocal = new RingBufferConfig((byte)t,(byte)20,null, FieldReferenceOffsetManager.RAW_BYTES);	
			
			final RingBuffer linesRing = new RingBuffer(linesRingConfigLocal);		
			LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
			
			baos.reset();
			ToOutputStreamStage reader = new ToOutputStreamStage(gm, linesRing, baos, false);
            
            StageScheduler scheduler = new ThreadPerStageScheduler(gm);
            
            scheduler.startup();
            
            boolean cleanExit = scheduler.awaitTermination(30, TimeUnit.SECONDS);	
			
			//return the position to the beginning to run the test again
			
			byte[] results = baos.toByteArray();
			if (null!=last) {
				int j = 0;
				int limit = Math.min(last.length,results.length);
				while (j<limit) {
					if (last[j]!=results[j]) {
						System.err.println("missmatch found at:"+j+" out of "+limit);	
						int context = 50;
						System.err.println("prev:"+new String(last,0, Math.min(j+context,limit)));
						System.err.println("next:"+new String(results,0, Math.min(j+context,limit)));
						
						
						break;
					}
					j++;
				}		
				assertEquals(last.length,results.length);
				assertTrue("Missed on "+t+" vs "+(t+1)+" at idx"+mismatchAt(last,results)+"\n",
						   Arrays.equals(last,  results));
			}
						
			last = Arrays.copyOf(results, results.length);
		
		}
		
	}


	private int mismatchAt(byte[] last, byte[] results) {
		int i = 0;
		int j = Math.min(last.length, results.length);
		while (i<j) {
			if (last[i]!=results[i]) {
				return i;
			}
			i++;
		}
		if (last.length!=results.length) {
			return i;
		}
		
		return -1;
	}


	private ByteBuffer generateCVSData(int bits) {
		int size = 1<<bits;
		ByteBuffer target = ByteBuffer.allocate(size);
		
		int i = 0;
		byte[] bytes = buildLine(i);		
		while (target.remaining() > bytes.length) {
			target.put(bytes);
			bytes = buildLine(++i);
		}
		target.flip();
		return target;
	}


	private byte[] buildLine(int i) {
		StringBuilder builder = new StringBuilder();
		
		builder.append(i).append(',').append("sometext").append(',').append(Integer.toHexString(i)).append('\n');
		
//		if (i<20) {
//			System.out.println("real test data:"+builder.toString());
//		}
		
		return builder.toString().getBytes();
	}


	private ByteVisitor buildLineTestingVisitor() {
		InputStream inputStream = SimpleParseTest.class.getResourceAsStream(TEST_FILE);
		if (null == inputStream) {
			try {
				inputStream = new FileInputStream(TEST_FILE);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				fail();
			}
		}
		
		final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				
		
		ByteVisitor testVisitor = new ByteVisitor() {
			boolean debug = false;
			
			
			@Override
			public void visit(byte[] data, int offset, int length) {
				
				try {
					byte[] expected = br.readLine().getBytes();
					assertTrue(new String(expected)+" found:"+new String(Arrays.copyOfRange(data, offset, length+offset)),Arrays.equals(expected, Arrays.copyOfRange(data, offset, length+offset)));
					if (debug) {
						System.err.println("AAA "+new String(data,offset,length));
					}
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}
				
			}
			
			@Override
			public void visit(byte[] data, int offset1, int length1, int offset2, int length2) {
				byte[] expected;
				try {
					expected = br.readLine().getBytes();
					assertTrue(new String(expected),Arrays.equals(Arrays.copyOfRange(expected, 0, 0+length1), Arrays.copyOfRange(data, offset1, offset1+length1)));
					assertTrue(new String(expected),Arrays.equals(Arrays.copyOfRange(expected, 0+length1, 0+length1+length2), Arrays.copyOfRange(data, offset2, offset2+length2)));
					if (debug) {
						System.err.println("BBB "+new String(data,offset1,length1)+new String(data,offset2,length2));
					}
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}
				
			}

			@Override
			public void close() {
				
				
			}};
		return testVisitor;
	}
	
	@Test
	public void testFieldReader() {
		ByteBuffer data = sourceBuffer.asReadOnlyBuffer();				
		RingBuffer linesRing = new RingBuffer(linesRingConfig);
		RingBuffer fieldsRing = new RingBuffer(fieldsRingConfig);
		linesRing.initBuffers();
		fieldsRing.initBuffers();
		
		GraphManager gm = new GraphManager();
		
		LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		lineSplitter.startup();
		lineSplitter.blockingRun();
		lineSplitter.shutdown();
				
		FieldSplitterStage fieldSplitter = new FieldSplitterStage(gm, linesRing, fieldsRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		fieldSplitter.startup();
		fieldSplitter.run();
		fieldSplitter.shutdown();
		
	
		
		int countBegins = 0;
		int countEnds = 0;
		int countFlush = 0;
		int countNull = 0;
		StringBuilder accumText = new StringBuilder();
		
		int[] expectedInt = new int[]{0,1970,1,2,934400,-420888,552000,2825600};
		long[] expectedLong = new long[]{10500000000l,-64600000000l};
		int i32 = 0;
		int i64 = 0; 
				
		while (RingReader.tryReadFragment(fieldsRing)) {
	        	assertTrue(RingReader.isNewMessage(fieldsRing));
	        	
	        	int msgLoc = RingReader.getMsgIdx(fieldsRing);
	        	
	        	String name = MetaMessageDefs.FROM.fieldNameScript[msgLoc];
	        	int templateId = (int) MetaMessageDefs.FROM.fieldIdScript[msgLoc];
	        	switch (templateId) {
	        		case 160: //beginMessage
	        			countBegins++;
	        		break;
	        		case 162: //endMessage
	        			countEnds++;
		        	break;
	        		case 62: //flush
	        			countFlush++;
	        	    break;
	        		case 128: //UInt32	        	
	        			int iValue = RingReader.readInt(fieldsRing, UINT32_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedInt[i32++],iValue);
	        	    break;
	        		case 130: //Int32	        	
	        			int isValue = RingReader.readInt(fieldsRing, INT32_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedInt[i32++],isValue);
	        	    break;
	        		case 132: //UInt64
	        			long lValue = RingReader.readLong(fieldsRing, UINT64_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedLong[i64++],lValue);	        			
	        			break;
	        		case 134: //Int64
	        			long lsValue = RingReader.readLong(fieldsRing, INT64_VALUE_LOC);
	        			if (fileSpecificTestValues) assertEquals(expectedLong[i64++],lsValue);	
	        			break;
	        		case 140: //Decimal
	        			int exp = RingReader.readDecimalExponent(fieldsRing, DOUBLE_VALUE_LOC);
	        			assertEquals("Test file only uses 2 places of accuracy",2,exp);
	        			
	        			long mant = RingReader.readDecimalMantissa(fieldsRing, DOUBLE_VALUE_LOC);	        			
	        			float value = RingReader.readFloat(fieldsRing, DOUBLE_VALUE_LOC);
	        			long expectedValue = (long)(Math.rint(value*100d));
	        			assertEquals("decimal and float versions of this field should be 'near' each other",expectedValue,mant);
	        	    break;
	        		case 136: //ASCII
	        			if (accumText.length()>0) {
	        				accumText.append(',');
	        			}
	        			RingReader.readASCII(fieldsRing, ASCII_VALUE_LOC, accumText);
	        		
		            break;
	        		case 164: //Null
	        			countNull++;
			        break;
	        	    default:
	        	    	fail("Missing case for:"+templateId+" "+name);
	        	
	        	}

	        	boolean debug = false;
	        	if (debug) {
	        		System.err.println(name+"  "+msgLoc+" "+templateId);
	        	}
	        	
		 }
		 assertEquals("There must be matching open and close messsage messages",countBegins,countEnds);
		 if (fileSpecificTestValues) assertEquals("Test data file was expected to use null on every row except one",countBegins-1, countNull); //only 1 row in the test data DOES NOT use null
		 assertEquals("End of test data should only send flush once",1,countFlush);
		 if (fileSpecificTestValues) {
			 assertEquals("One of the ASCII fields does not match",
					 	"NYSE:HPQ,Hewlett-packard Co,NYSE:ED,Consolidated Edison Inc,NYSE:AEP,American Electric Power,NYSE:GT,Goodyear Tire & Rubber,NYSE:KO,Coca-cola Co,NYSE:MCD,Mcdonald's Corp",
					 	accumText.toString()); 
		 }
	}
	
	
	@Test
	public void testIngestTemplate() {
		ByteBuffer data = sourceBuffer.asReadOnlyBuffer();				
		RingBuffer linesRing = new RingBuffer(linesRingConfig);
		RingBuffer fieldsRing = new RingBuffer(fieldsRingConfig);
		RingBuffer flatFileRing = new RingBuffer(flatFileRingConfig);
		
		linesRing.initBuffers();
		fieldsRing.initBuffers();
		flatFileRing.initBuffers();
		
		GraphManager gm = new GraphManager();
		
		LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		lineSplitter.startup();
		lineSplitter.blockingRun();
		lineSplitter.shutdown();
				
		FieldSplitterStage fieldSplitter = new FieldSplitterStage(gm, linesRing, fieldsRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		fieldSplitter.startup();
		fieldSplitter.run();
		fieldSplitter.shutdown();
		
		TemplateGeneratorStage generatorSplitter = new TemplateGeneratorStage(gm, fieldsRing, flatFileRing);
		
		//this assumes that the ring buffer is large enough to hold the small test file
		generatorSplitter.startup();
		generatorSplitter.run();
		generatorSplitter.shutdown();
				
		System.out.println("finished run");
		
		File temp;
		try {
			temp = File.createTempFile("testTemplate", "xml");
			FileWriteStage fileWriter = new FileWriteStage(gm, flatFileRing, temp);
			fileWriter.startup();
			fileWriter.run();	
			fileWriter.shutdown();
			
			//only needs to validate that we wrote something of some size
			assertTrue(temp.length()>10);
			System.err.println(temp.getAbsolutePath()+" size "+temp.length());
			
			//loading and parsing the file to ensure it was good
			byte[] catBytes = TemplateLoader.buildCatBytes(temp.getAbsolutePath(),  new ClientConfig());
			TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
			FieldReferenceOffsetManager from = catalog.getFROM();
			assertTrue("cat bytes length was :"+catBytes.length+" catalog:"+temp.getAbsolutePath(), from.fieldIdScript.length>0);
						
			
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		
		
	}
	
	/**
	 * This is the example you are looking for
	 */
	@Test
	public void testIngestTemplateThreaded() {
		
		//output target template file
		File temp =null;
		try {
			temp = File.createTempFile("testTemplate", "xml");
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		//input test data
		ByteBuffer data = sourceBuffer.asReadOnlyBuffer();				

		//build all the ring buffers
		RingBuffer linesRing = new RingBuffer(linesRingConfig);
		RingBuffer fieldsRing = new RingBuffer(fieldsRingConfig);
		RingBuffer flatFileRing = new RingBuffer(flatFileRingConfig);
		
		GraphManager gm = new GraphManager();
		
		//build all the stages
		LineSplitterByteBufferStage lineSplitter = new LineSplitterByteBufferStage(gm, data, linesRing);
		FieldSplitterStage fieldSplitter = new FieldSplitterStage(gm, linesRing, fieldsRing);
		TemplateGeneratorStage generatorSplitter = new TemplateGeneratorStage(gm, fieldsRing, flatFileRing);
		FileWriteStage fileWriter = new FileWriteStage(gm, flatFileRing, temp);

		//put the stages into the executors
		executeInParallel(gm);			

		//loading and parsing the file to ensure it was good
		byte[] catBytes = TemplateLoader.buildCatBytes(temp.getAbsolutePath(),  new ClientConfig());
		TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
		FieldReferenceOffsetManager from = catalog.getFROM();
		assertTrue(from.fieldIdScript.length>0);
			
	}



	public static void executeInParallel(GraphManager gm) {
		
		
		GraphManager.enableBatching(gm);
		
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		 
		scheduler.startup();
		
        boolean cleanExit = scheduler.awaitTermination(100, TimeUnit.SECONDS);

		
	}
	
	
	
}
