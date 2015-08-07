package com.ociweb.pronghorn.mapping.hacktest;

public class MyReset {

	private final String version;
	
//	@Pronghorn(
//	messsage = "Reset",
//	fields = {"Version"}
//)
	public MyReset(String version) {
		this.version = version;
	}
	
	public String version() {
		return version;
	}
	
}
