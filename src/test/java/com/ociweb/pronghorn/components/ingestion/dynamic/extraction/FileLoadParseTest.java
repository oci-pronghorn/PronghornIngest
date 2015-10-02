package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Before;
import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;


public class FileLoadParseTest {

	File testFile;
    Pipe rb;
	
	
	@Before
	public void createTestFile() {
				
		rb = new Pipe(new PipeConfig((byte)21, (byte)7, null, FieldReferenceOffsetManager.RAW_BYTES));
		rb.initBuffers();
		try {
			
			//build a bunch of data
			int i = 1<<26;			
			byte[] temp = new byte[i];
			while (--i>=0) {
				if ((i&0x3F)==0) {
					temp[i] = '\n';					
				} else {
					if ((((i+1)&0x7)==0) && (((i+9)&0x1F)==0)) {
						temp[i] = '\\';
					} else {					
						if ((i&0x7)==0) {
							temp[i] = ',';
						} else {						
							temp[i]= (byte)('0'+(int)(0x1F&i));
						}		
					}
				}
			}
			
			//System.err.println(new String(temp));
			
			//write the data to the file
			File f = File.createTempFile(this.getClass().getSimpleName(), "test");
			f.deleteOnExit();//but do keep it arround while we do our test
						
			FileOutputStream out = new FileOutputStream(f);
			out.write(temp);
			out.close();
		
			testFile = f;
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}
	
	
    @Test
    public void testFileLoad() {
    	
    	try {
			FileChannel fileChannel = new RandomAccessFile(testFile, "r").getChannel();
			
			long fileSize = fileChannel.size();
			
			
			long startTime = System.currentTimeMillis();
			extract(fileChannel);
			long duration = System.currentTimeMillis()-startTime;
			
			long bitsPerMS = (long)((fileSize*8)/duration);
			long gBitsPerSecond = bitsPerMS/1000000;  // *1000 /1000000000   
			
			System.out.println(bitsPerMS+" bits per MS");
			System.out.println(gBitsPerSecond+" Gbits per second");
			
			
    	} catch (Exception e) {			
			e.printStackTrace();
			fail();
		}  
    	
    }
	
	/**
	 * Experimental parser built to leverage multiple cores and keep up with the speed of modern SSDs
	 * 
	 * @param fileChannel
	 * @throws IOException
	 */
    public void extract(FileChannel fileChannel) throws IOException {
        MappedByteBuffer mappedBuffer;
        
        long fileSize = fileChannel.size();
        long position = 0;
        int tailPadding = 8;//needed to cover the transition
        long blockSize = 1<<25;

        TypeExtractor typeExtractor = new TypeExtractor(true /* force ASCII */);
        RecordFieldExtractor rfe = new RecordFieldExtractor();

        
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(blockSize, fileSize-position));
        int padding = tailPadding;
        do {       
                        
            if (mappedBuffer.limit()+position==fileSize) {
                padding = 0;
            }
        

            int pos = 0;
            
            Pipe.setValue(rb.slabRing, rb.mask, Pipe.getWorkingHeadPositionObject(rb).value++, pos);
            
            int tokenCount = 0;
            int c =0;
            
            int j = mappedBuffer.remaining()-padding;
            do { 
            	//walk over the data while we have this section mapped.
            	c++;
            	
            	byte b = (byte)mappedBuffer.get();
            	
      //      	RecordFieldExtractor.appendContent(rfe, b); //TOO much work here must do on reading thread.

            	//TODO: check the field type sums
            	//TODO: zero copy but we need to discover tokens
            	
            	//splits on returns, commas, dots and many other punctuation
            	if (b < 48) {
            		//System.err.println("char :"+b);
            		
            		//what mask can be built to combine the byte we are after.
            		
            	//	allTheBits++; //do something
            		
            		pos = mappedBuffer.position();
            		Pipe.setValue(rb.slabRing, rb.mask, Pipe.getWorkingHeadPositionObject(rb).value++, pos);
            		
            		if ((++tokenCount&0xF)==0) {
            			Pipe.publishWrites(rb);
 
            		}
            		
            		
            	//	rb.reset();
            		
            	}
            	
            	
            } while (--j>0);
            
            //this tokenizer assumes that the file ends with a field delimiter so the last record gets flushed.
            
            //TODO: need to wait for threads to finish before swapping to new page or have multiple pages to swap in/out
            
            //only increment by exactly how many bytes were read assuming we started at zero
            //can only cut at the last known record start
            position+=c;
            
            
            System.out.println("bytes read so far:"+position);
            
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(blockSize, fileSize-position));
            
        } while (position<fileSize);

    }


	private void appendChar(byte b) {
		// TODO Auto-generated method stub
		
	}
	
	
	
}
