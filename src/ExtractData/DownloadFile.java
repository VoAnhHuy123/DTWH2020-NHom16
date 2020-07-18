package ExtractData;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class DownloadFile {
	private static PreparedStatement ps = null;
	Connection con;
	private static String from = "datawarehouse0126@gmail.com";
//	private static String to = "huyvo2581999@gmail.com";
	private static String to = "tranghoang13199@gmail.com";
	private static String passfrom = "datawarehouse2020";
//	private static String content = ";
	static String mess;
	private static String subject = "Update log successfull: DATA WAREHOUSE SERVER";

	static {
		try {
			System.loadLibrary("chilkat");// copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void getTrial() {
		CkGlobal glob = new CkGlobal();
		boolean success = glob.UnlockBundle("Anything for 30-day trial");
		if (success != true) {
			System.out.println(glob.lastErrorText());
			return;
		}
		int status = glob.get_UnlockStatus();
		if (status == 2) {
			System.out.println("Unlocked using purchased unlock code.");
		} else {
			System.out.println("Uncloked in trail mode.");
		}
		System.out.println(glob.lastErrorText());
	}

	public static boolean downloadFile(String host, int ports, String user, String pass, String path, String local,
			String file_name, String file_type) throws ClassNotFoundException, SQLException {
		DownloadFile d = new DownloadFile();
		d.getTrial();
		CkSsh ssh = new CkSsh();

		String hostname = host;


		int port = ports;

		// Connect to an SSH server:
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			mess = "Faild: Host and port invalid";
			return false;
		}

		// Wait a max of 5 seconds when reading responses..
		ssh.put_IdleTimeoutMs(5000);

		// Authenticate using login/password:
		success = ssh.AuthenticatePw(user, pass);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			mess = "Faild: User and pass invalid";
			return false;
		}

		// Once the SSH object is connected and authenticated, we use it
		// in our SCP object.
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return false;
		}


		scp.put_SyncMustMatch(file_name + "*.*" + file_type);
		String remotePath = path;
		String localPath = local; // thu muc muon down file ve
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);

		if (success != true) {
			System.out.println(scp.lastErrorText());
			return false;
		}

		System.out.println("SCP download file success.");

		// Disconnect
		ssh.Disconnect();
		return true;

	}

	// ***** DOWNLOAD TAT CA CAC FILE CUA CAC NHOM VE LOCAL ********//

	public static void getLog(int id) throws ClassNotFoundException, SQLException {
		Connection con;
		PreparedStatement pre;

		// ket noi toi controldb
		con = ConnectionDB.getConnection("controldb");
		String sql = "SELECT * FROM `table_config` WHERE config_id = ?";
		pre = con.prepareStatement(sql);
		pre.setInt(1, id);
		System.out.println();
		ResultSet tmp = pre.executeQuery();
		// duyet record trong resultset
		while (tmp.next()) {
			String file_name = tmp.getString("file_name");
			String file_type = tmp.getString("file_type");
			String local = tmp.getString("source");
			String user = tmp.getString("userRemote");
			String pass = tmp.getString("passRemote");
			String path = tmp.getString("remotePath");
			String host = tmp.getString("host");
			int ports = tmp.getInt("port");

			// bat dau load file ve
			boolean download = new DownloadFile().downloadFile(host, ports, user, pass, path, local, file_name,
					file_type);
			Staging s = new Staging();

			if (download) {
				File file = new File(local);
				System.out.println("File");
				try {
					// Kiem tra xem file co ton tai trong thu muc hay khong
					if (file.isDirectory()) {

						File[] listFile = file.listFiles();
						for (int i = 0; i < listFile.length; i++) {
							int numberOfLine = s.countLines(listFile[i]);
							/// bat dau insert vao table_log
							setupLog(listFile[i].getName(), "ER", numberOfLine, id);
							// Thong bao thanh cong
							System.out.println("Insert success full: "+listFile[i]);
							// gui mail
						}
						SendMail send = new SendMail(from, to, passfrom,
								" Update log successfull from " + path + " to " + local + " at "
										+ new Timestamp(System.currentTimeMillis()).toString().substring(0, 19),
								subject);
						send.sendMail();

					} else
						System.out.println("No fine path");

//					br.close();
//					con.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else {
				// thông báo ra màn hình
				System.out.println("DOWNLOAD KHONG THANH CONG");
				// Cập nhật file_status là ERROR và thời gian download là thời
				// gian hiện tại
				String sql1 = "INSERT INTO table_log (file_name,data_file_config_id,file_status,file_timestamp) VALUES (?,?,?,?)";

				try {
					ps = ConnectionDB.getConnection("controldb").prepareStatement(sql1);
					ps.setString(1, file_name);
					ps.setInt(2, id);
					ps.setString(3, "ERROR");
					ps.setString(4, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
					ps.executeUpdate();
				} catch (ClassNotFoundException | SQLException e1) {
					e1.printStackTrace();
				}
				// gửi mail về hệ thống thông báo lỗi
				SendMail send = new SendMail(from, to, passfrom, "Updated log faild: Error " + mess + ".",
						"Updated log Faild: DATA WAREHOUSE SERVER");
				send.sendMail();
			}
		}
	}

	private static void setupLog(String name, String status, int numberOfLine, int id)
			throws SQLException, ClassNotFoundException {
		String query = "INSERT INTO table_log (file_Name,file_timestamp, file_status, staging_load_count, data_file_config_id) VALUES (?,?,?,?,?)";
		PreparedStatement st = ConnectionDB.getConnection("controldb").prepareStatement(query);
		st.setString(1, name);
		st.setString(2, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
		st.setString(3, status);
		st.setInt(4, numberOfLine);
		st.setInt(5, id);
		st.execute();

	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		DownloadFile d = new DownloadFile();
		d.getLog(4);
	}
}
