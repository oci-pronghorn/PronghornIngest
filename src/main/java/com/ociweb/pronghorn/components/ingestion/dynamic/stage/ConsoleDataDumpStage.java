package com.ociweb.pronghorn.components.ingestion.dynamic.stage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorToJSON;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConsoleDataDumpStage extends PronghornStage {


    private static Logger log = LoggerFactory.getLogger(ConsoleDataDumpStage.class);
    
	private final Pipe input;
	
	private StreamingReadVisitor visitor;
	private StreamingVisitorReader reader;
	private FieldReferenceOffsetManager from;
	private final int maxString;
	
	//TODO: AA, move this into the unit tests.
	public ConsoleDataDumpStage(GraphManager graphManager, Pipe input, int maxString) {
		super(graphManager, input, NONE);
		this.input = input;
		this.maxString = maxString;
	}

	@Override
	public void startup() {
		super.startup();		
		
		try{
			
			from = Pipe.from(input);
			visitor = new StreamingReadVisitorToJSON(System.out) {
				@Override
				public void visitASCII(String name, long id, Appendable value) {
					assert (((CharSequence)value).length()<maxString) : "Text is too long found "+((CharSequence)value).length();
					super.visitASCII(name, id, value);
				}
				@Override
				public void visitUTF8(String name, long id, Appendable value) {
					assert (((CharSequence)value).length()<maxString) : "Text is too long found "+((CharSequence)value).length();
					super.visitUTF8(name, id, value);
				}
			};
						
			reader = new StreamingVisitorReader(input, visitor );
			
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER TARGET FOR INFORMATION
			//////
			
			reader.startup();
								
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	
	@Override
	public void run() {
		reader.run();
	}
	

	@Override
	public void shutdown() {
		
		try{
			reader.shutdown();
			
		    ///////
			//PUT YOUR LOGIC HERE TO CLOSE CONNECTIONS FROM THE DATABASE OR OTHER TARGET OF INFORMATION
			//////
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		} 
	}


}
