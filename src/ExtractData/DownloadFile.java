package ExtractData;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class DownloadFile {
	private static PreparedStatement ps = null;
	Connection con;
	private String from = "datawarehouse0126@gmail.com";
	private String to = "tranghoang13199@gmail.com";
	private String passfrom = "datawarehouse2020";
	private String mess;
	private String subject = "Update log successfull: DATA WAREHOUSE SERVER";

	static {
		try {
			System.loadLibrary("chilkat");// copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public boolean downloadFile(String host, int ports, String user, String pass, String path, String local,
			String file_name, String file_type) throws ClassNotFoundException, SQLException {
		// 5. Kết nối tới FTP server chotructuyenco
		CkSsh ssh = new CkSsh();
		// dia chi tren drive: drive.ecepvn.org
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
		// đợi tối đa 5 giây khi đọc phản hồi
		ssh.put_IdleTimeoutMs(5000);

		// Authenticate using login/password:
		// xac thuc dang nhap voi user: guest_access voi pass: 123456
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
		// lay cac file có ten bat dau bang sinh vien, mon hoc.... với duoi la .xlsx
		scp.put_SyncMustMatch(file_name + "*.*" + file_type);
		String remotePath = path;// dia chi de lay file download ve /volume1/ECEP/song.nguyen/DW_2020/data
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

	public void getLog(String id) throws ClassNotFoundException, SQLException {
		Connection con;
		PreparedStatement pre;

		// 1. Mở kết nối tới database "controndb"
		con = ConnectionDB.getConnection("controldb");
		// 2.Tìm các file có trong table_config theo id
		String sql = "SELECT target_table,file_type,source,userRemote,passRemote,remotePath,host,port FROM `table_config` WHERE config_id = ?";
		pre = con.prepareStatement(sql);
		pre.setString(1, id);
		System.out.println();
		// 3. Nhận ResultSet chức các record thoả yêu cầu
		ResultSet tmp = pre.executeQuery();
		// 4. Duyệt record trong resultset
		while (tmp.next()) {
			String target_table = tmp.getString("target_table");
			String file_type = tmp.getString("file_type");
			String local = tmp.getString("source");
			String user = tmp.getString("userRemote");
			String pass = tmp.getString("passRemote");
			String path = tmp.getString("remotePath");
			String host = tmp.getString("host");
			int ports = tmp.getInt("port");
			// 6. Gọi phương thức downloadFile(host, ports, user, pass, path, local,
			// target_table,file_type) để download file về local
			// Bắt đầu download file về
			boolean download = new DownloadFile().downloadFile(host, ports, user, pass, path, local, target_table,
					file_type);
			// Nếu thành công
			if (download) {
				// 6.2.1 thông báo ra màn hình
				System.out.println("DOWNLOAD THANH CONG");
				// lay file trong thu muc
				File file = new File(local);
				try {
					if (file.isDirectory()) {
						// 6.2.2 lay ra danh sach cac file co trong thu muc
						File[] listFile = file.listFiles();
						// 6.2.3 duyet trong log xem file da duoc ghi log chua
						String sqllog = "SELECT file_name FROM table_log";
						PreparedStatement pslog = con.prepareStatement(sqllog);
						ResultSet rslog = pslog.executeQuery();
						// 7.2.1.2 ds các file co trong log
						ArrayList<String> listFileLog = new ArrayList<>();
						while (rslog.next()) {
							listFileLog.add(rslog.getString("file_name"));
						}
						for (File f : listFile) {
							// neu trong log chua ton tai file thì bat dau insert vao
							if (!listFileLog.contains(f.getName())) {
								// dem so dong co trong file
								int numberOfLine = countLines(f);
								// 6.2.3.1.1 bat dau ghi log
//							setupLog(f.getName(), "ER", numberOfLine, id);
								String query = "INSERT INTO table_log (file_Name,file_timestamp, file_status, staging_load_count, data_file_config_id) VALUES (?,?,?,?,?)";
								PreparedStatement st = ConnectionDB.getConnection("controldb").prepareStatement(query);
								st.setString(1, f.getName());
								st.setString(2, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
								st.setString(3, "ER");
								st.setInt(4, numberOfLine);
								st.setString(5, id);
								st.execute();

								// 6.2.3.1.2 Đưa ra thông báo
								System.out.println("Insert success full: " + f);
							} else
								// 6.2.3.2.1 Đưa ra thông báo
								System.out.println(f + " : Đã được insert vào log ");
						}
						// 6.2.3.1.3 gui mail
						SendMail send = new SendMail(from, to, passfrom,
								" Update log successfull from " + path + " to " + local + " at "
										+ new Timestamp(System.currentTimeMillis()).toString().substring(0, 19),
								subject);
						send.sendMail();

					} else
						// Thông báo file không tồn tại
						System.out.println("No fine path");
				} catch (Exception e) {
					e.printStackTrace();
				}
				/// 7. Đóng tất cả kết nối tới Database "controldb"
				finally {
					if (ps != null) {
						try {
							ps.close();
							con.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}

			} else {
				// 6.1.1 Thông báo ra màn hình
				System.out.println("DOWNLOAD KHONG THANH CONG");
				// 6.1.2 Cập nhật file_status là ERROR và thời gian download là thời gian hiện
				// tại
				String sql1 = "INSERT INTO table_log (file_name,data_file_config_id,file_status,file_timestamp) VALUES (?,?,?,?)";

				try {
					ps = con.prepareStatement(sql1);
					ps.setString(1, target_table);
					ps.setString(2, id);
					ps.setString(3, "ERROR");
					ps.setString(4, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
					ps.executeUpdate();
				} catch (SQLException e1) {
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
				// 6.1.3 Gửi mail về hệ thống thông báo lỗi
				SendMail send = new SendMail(from, to, passfrom, "Updated log faild: Error " + mess + ".",
						"Updated log Faild: DATA WAREHOUSE SERVER");
				send.sendMail();
			}
		}
	}

	// dem so dong co trong file
	static int countLines(File file)
			throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		// bien dung de dem
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (!file.exists()) {
				System.out.println("File không tồn tại");
			} else {
				// tạo phiên bản Workbook tham chiếu đến tệp .xlsx
				workBooks = new XSSFWorkbook(file);
				// tạo một đối tượng Trang tính để truy xuất đối tượng
				XSSFSheet sheet = workBooks.getSheetAt(0);
				// lặp qua tệp excel
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
		for (int i = 0; i < args.length; i++) {
			String id = args[i];
			d.getLog(id);
		}
	}
}
