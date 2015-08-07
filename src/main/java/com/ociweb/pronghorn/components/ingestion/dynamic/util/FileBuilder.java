package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileBuilder implements Appendable {

	private final BufferedOutputStream bost;
	
	public FileBuilder(File file) {
		try {
			FileOutputStream fost = new FileOutputStream(file);
			bost = new BufferedOutputStream(fost, 1<<16);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	

	@Override
	public Appendable append(CharSequence csq) throws IOException {			
		append(csq,0,csq.length());
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end) throws IOException {
		int j = start;
		while (j<end) {
			append(csq.charAt(j++));
		}
		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		//this implementation assumes ASCII strings !
		bost.write(c);
		return this;
	}

	public void close() {
		try {
			bost.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
