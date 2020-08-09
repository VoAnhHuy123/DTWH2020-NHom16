package ExtractData;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;

public class LoadStagin {
	public static void main(String[] args) throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		Staging s = new Staging();
		s.staging();
	}
}    
