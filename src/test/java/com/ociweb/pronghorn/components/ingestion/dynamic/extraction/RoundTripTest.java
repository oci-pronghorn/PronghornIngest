package com.ociweb.pronghorn.components.ingestion.dynamic.extraction;

public class RoundTripTest {
	    // !!! errors. see the end of file for details

    //@Test
    // public void runTest() {
	// 	//load the data file
        // URL sourceData = getClass().getResource("/roundtrip/small.csv");
        // File csvFile = new File(sourceData.getFile().replace("%20", " "));

        // //load this template
	// 	String templateFile = "/roundtrip/smallTemplate.xml";
		
	// 	//encode data into this temp file
	// 	File targetTemp;
	// 	try {
	// 		targetTemp = File.createTempFile("test", "fast");
	// 		RecordFieldExtractor typeAccum = new RecordFieldExtractor(); 
	// 		IngestUtil.encodeGivenTemplate(csvFile, templateFile, typeAccum, targetTemp.getAbsolutePath());
	// 	} catch (IOException e) {
	// 		e.printStackTrace();
	// 		fail();
	// 	}
		
	// 	//re-open the temp file and decode the data
		
		
		
		
		
	// }
	
	
}

//     Templates:/roundtrip/smallTemplate.xml
// loading catalog with 2 entries
// 	****************** open frame:
//     new catalog has been written to the stream as template ID:0
// 7 now failure on field 1
// 	[16, 0, 0, 0, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -2147483584, 1, 0, 0, 0, 0, 1073741920, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
// 0 0
// 1 0
// 2 1000000000000000000000011000000
// 3 0
// 4 0
// 5 0
// 6 0
// 7 0
// 8 0
// 9 0
// 10 0
// 11 0
// 12 0
// 13 0
// 14 0
// 	AA unable to find non null! at 176 data:[0, 0, 1073742016, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
// 15 0 0
// 14 0 0
// 13 0 0
// 12 0 0
// 11 0 0
// 10 0 0
// 9 0 0
// 8 0 0
// 7 0 0
// 6 0 0
// 5 0 0
// 4 0 80
// 3 0 0
// 2 0 0
// 1 0 0
// 0 0 16
// 	    assumed type of :7 not found ************************* ERROR origType:7
// 		last closed line: NYSE:MCD,Mcdonald's Corp,,43.00,44.25,43.00,44.25,2825600
// java.lang.Exception: Require optional field but unable to find one in field 1 records 7 example 
