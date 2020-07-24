package ExtractData;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

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

	public static void getLog(String id) throws ClassNotFoundException, SQLException {
		Connection con;
		PreparedStatement pre;

		// ket noi toi controldb
		con = ConnectionDB.getConnection("controldb");
		String sql = "SELECT * FROM `table_config` WHERE config_id = ?";
		pre = con.prepareStatement(sql);
		pre.setString(1, id);
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
			if (download) {
				File file = new File(local);
				System.out.println("File");
				try {
					// Kiem tra xem file co ton tai trong thu muc hay khong
					if (file.isDirectory()) {
						//
						File[] listFile = file.listFiles();
						// duyet tung file
						for (int i = 0; i < listFile.length; i++) {
							// dem so dong du lieu cua tung file
							int numberOfLine = countLines(listFile[i]);
							/// bat dau insert vao table_log
							setupLog(listFile[i].getName(), "ER", numberOfLine, id);
							// Thong bao thanh cong
							System.out.println("Insert success full: " + listFile[i]);
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
				} finally {
					if (ps != null) {
						try {
							ps.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
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
					ps.setString(2, id);
					ps.setString(3, "ERROR");
					ps.setString(4, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
					ps.executeUpdate();
				} catch (ClassNotFoundException | SQLException e1) {
					e1.printStackTrace();
				} finally {
					if (ps != null) {
						try {
							ps.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				// gửi mail về hệ thống thông báo lỗi
				SendMail send = new SendMail(from, to, passfrom, "Updated log faild: Error " + mess + ".",
						"Updated log Faild: DATA WAREHOUSE SERVER");
				send.sendMail();
			}
		}
	}

	private static void setupLog(String name, String status, int numberOfLine, String id)
			throws SQLException, ClassNotFoundException {
		String query = "INSERT INTO table_log (file_Name,file_timestamp, file_status, staging_load_count, data_file_config_id) VALUES (?,?,?,?,?)";
		PreparedStatement st = ConnectionDB.getConnection("controldb").prepareStatement(query);
		st.setString(1, name);
		st.setString(2, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
		st.setString(3, status);
		st.setInt(4, numberOfLine);
		st.setString(5, id);
		st.execute();

	}

	static int countLines(File file)
			throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (!file.exists()) {
				System.out.println("File không tồn tại");
			} else {
				workBooks = new XSSFWorkbook(file);
				XSSFSheet sheet = workBooks.getSheetAt(0);
				Iterator<Row> rows = sheet.iterator();
				rows.next();
				while (rows.hasNext()) {
					rows.next();
					result++;
				}
				return result;
			}
		} catch (IOException | org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
			e.printStackTrace();
		} finally {
			if (workBooks != null) {
				try {
					workBooks.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		DownloadFile d = new DownloadFile();
		String id = args[0];
		d.getLog(id);
		String id1 = args[1];
		d.getLog(id1);
		String id2 = args[2];
		d.getLog(id2);
		String id3 = args[3];
		d.getLog(id3);
	}
}
