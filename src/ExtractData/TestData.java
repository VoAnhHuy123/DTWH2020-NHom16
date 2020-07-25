package ExtractData;

import java.sql.SQLException;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;

public class TestData {
	public static void main(String[] args) throws ClassNotFoundException, SQLException, InvalidFormatException,
			org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		DownloadFile d = new DownloadFile();
		Staging s = new Staging();
		String id = args[0];
		d.getLog(id);
		s.staging();
		String id1 = args[1];
		d.getLog(id1);
		s.staging();
		String id2 = args[2];
		d.getLog(id2);
		s.staging();
		String id3 = args[3];
		d.getLog(id3);
	}
}
