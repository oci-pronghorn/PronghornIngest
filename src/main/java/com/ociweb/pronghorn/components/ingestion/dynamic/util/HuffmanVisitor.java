package com.ociweb.pronghorn.components.ingestion.dynamic.util;

public interface HuffmanVisitor {

	void visit(int value, int frequency, long code);

}
