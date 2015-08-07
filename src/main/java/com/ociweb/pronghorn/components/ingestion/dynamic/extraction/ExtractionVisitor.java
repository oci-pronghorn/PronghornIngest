package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

import java.nio.ByteBuffer;

@Deprecated
public interface ExtractionVisitor {

    void openFrame(); //called before first use on new frame
    
    void appendContent(ByteBuffer mappedBuffer, int contentPos, int position, boolean contentQuoted);

    void closeRecord(ByteBuffer mappedBuffer, int startPos);

    boolean closeField(ByteBuffer mappedBuffer, int startPos);

    void closeFrame(); //must use any buffers it has been given because they are about to be changed
    
    void debug(ByteBuffer mappedBuffer, int startPos);

}
