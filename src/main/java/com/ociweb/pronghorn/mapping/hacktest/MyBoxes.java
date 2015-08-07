package com.ociweb.pronghorn.mapping.hacktest;

public class MyBoxes {

	private final int count;
	private final String owner;
	
//	@Pronghorn(
//	messsage = "Boxes",
//	fields = {"Count","Owner"}
//)
	public MyBoxes(int count, String owner) {
		this.count = count;
		this.owner = owner;
	}	
	
	public int getCount() {
		return count;
	}
	
	public String getOwner() {
		return owner;
	}
}
