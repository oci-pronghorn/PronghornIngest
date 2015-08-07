package com.ociweb.pronghorn.components.ingestion.dynamic.util;

public interface BloomCountVisitor {

	void visit(int hash, int count);

}
