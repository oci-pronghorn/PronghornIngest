package com.ociweb.pronghorn.components.ingestion.dynamic.stage;

import org.junit.Test;

import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;

public class MetaFieldFROMTest {

	@Test
	public void instanceTest() {
		// ensure that metaTemplate.xml is successfully parsed and a FROM created
		FieldReferenceOffsetManager from = MetaMessageDefs.FROM;
	}
	
	
}
