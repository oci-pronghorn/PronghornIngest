package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

public class CatByteConstantProvider implements CatByteProvider {

	private final byte[] catBytes;
	
	public CatByteConstantProvider(byte[] catBytes) {
		this.catBytes = catBytes;
	}
	
	@Override
	public byte[] getCatBytes() {
		return catBytes;
	}

}
