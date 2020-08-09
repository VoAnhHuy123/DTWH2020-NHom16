package ExtractData;

import java.sql.SQLException;

public class Dowload {
	public static void main(String[] args) {
		DownloadFile d = new DownloadFile();
		for (int i = 0; i < args.length; i++) {
			String id = args[i];
			try {
				d.getLog(id);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
//		try {
//			d.getLog("3");
//		} catch (ClassNotFoundException | SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
