package com.ociweb.pronghorn.mapping.hacktest;

import java.util.Date;

public class MyCargoSample {

	private Date date;
	private double weight;
	
	
//	@Pronghorn(
//				messsage = "Sample",
//				fields = {"Year,Month,Day","Weight"}
//			)
	public MyCargoSample(Date date, double weight) {
		this.date = date;
		this.weight = weight;		
	}
	
	public Date getDate() {
		return date;
	}
	
	public double getWeight() {
		return weight;
	}
}
