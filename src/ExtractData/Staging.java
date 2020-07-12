package ExtractData;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.format.CellFormatType;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Staging {
	static final String NUMBER_REGEX = "^[0-9]+$";
	static ArrayList<String> columnslist = new ArrayList<String>();
	static int numOfcol;
	static String column_list;
	static String dilimeter;
	static String target_table;
	static String source;
	static String source_file;
	static String local;
	static int countofline;
	private static String from = "datawarehouse0126@gmail.com";
//	private static String to = "huyvo2581999@gmail.com";
	private static String to = "huyvo2581999@gmail.com";
	private static String passfrom = "datawarehouse2020";
//	private static String content = ";
	static String mess;
	private static String subject = "Update log successfull: DATA WAREHOUSE SERVER  ";

	public void staging() throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		Connection con;
		PreparedStatement pre;
		// Kiem tra ket noi
		try {
			con = ConnectionDB.getConnection("controldb");
			String sql = "SELECT * FROM `table_config` JOIN table_log ON table_config.config_id = table_log.data_file_config_id WHERE table_log.file_status = 'ER'";
			pre = con.prepareStatement(sql);
			ResultSet rs = pre.executeQuery();
			rs.next();
			String succeed ="";
			String fail="";
			int id = rs.getInt("id");
			target_table = rs.getString("target_table");
			source = rs.getString("source");
			column_list = rs.getString("column_list");
//			System.out.println(column_list);
			numOfcol = rs.getInt("numofcol"); 
			dilimeter = rs.getString("delimeter");
			source_file = null;
			local = rs.getString("source");
			String variables = rs.getString("variables");
			StringTokenizer tokens = new StringTokenizer(column_list, dilimeter);
			createTable(target_table, variables, column_list);
			while (tokens.hasMoreTokens()) {
				columnslist.add(tokens.nextToken());
			}

			String selectAll = "select * from table_log";
			System.out.println("ok");
			PreparedStatement getAll = ConnectionDB.getConnection("controldb").prepareStatement(selectAll);
			ResultSet result = getAll.executeQuery();
			while (result.next()) {
				String status = result.getString("file_status");
				if (status.equals("ER")) {
					System.out.println("++++++++++++++++");
					System.out.println(result.getString("file_name"));
					System.out.println("++++++++++++++++");
					String fileName = result.getString("file_name");
					File file = new File(local + "\\" + fileName);
					if (!file.exists()) {
						System.out.println("File không tồn tại");
					} else {
						/// doc file
						String readFile = Staging.readValuesXLSX(file);
						System.out.println("Doc file thanh cong");
						String values = "";
						System.out.println("========");

						// PreparedStatement newTable =
						// ConnectionDB.getConnection("staging").prepareStatement(sql);
						
						if (readFile != null) {
							System.out.println("Chuan bi insert du lieu");
							// connect to stagin

							// end
							if (Staging.writeDataToBD(target_table, column_list, readFile)) {
								countofline = Staging.countLines(file);
								if (countofline > 0) {
									/// Kiem tra so dong duoc load vao staging
									String sqlupdate = "UPDATE table_log SET file_status = ?,staging_load_count =?, file_timestamp =? WHERE id = ?";

									try {
										pre = ConnectionDB.getConnection("controldb").prepareStatement(sqlupdate);
										pre.setString(1, "TR");
										pre.setInt(2, countofline);
										pre.setString(3,
												new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
										pre.setInt(4, result.getInt("id"));
										pre.execute();
										succeed+=fileName+" ";
										System.out.println("Update success.......");
									} catch (ClassNotFoundException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (SQLException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}else {
								String sqlupdate = "UPDATE table_log SET file_status = ?,staging_load_count =?, file_timestamp =? WHERE id = ?";

								try {
									pre = ConnectionDB.getConnection("controldb").prepareStatement(sqlupdate);
									pre.setString(1, "Fail");
									pre.setInt(2, countofline);
									pre.setString(3,
											new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
									pre.setInt(4, result.getInt("id"));
									pre.execute();
									fail +=fileName+" ";
									System.out.println("Update success.......");
								} catch (ClassNotFoundException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								
								//send mail
								
							}
						}
//					}
//				} else {
//					createTable(target_table, variables, column_list);
//					System.out.println("tao bang moi");
					}
				}
			}
			SendMail send = new SendMail(from, to, passfrom,
					"Extract to staging: Succeed: " + succeed + " \n" + "Fail:" + fail + "\n"
							+ new Timestamp(System.currentTimeMillis()).toString().substring(0, 19),
					subject);
			send.sendMail();
			
		} catch (ClassNotFoundException | SQLException e) {
			System.out.println("khong co trang thai san sang");
		}
	}

//	public boolean checkDBExists(String table_name) {
//
//		try {
//
//			DatabaseMetaData dbm = ConnectionDB.getConnection("staging").getMetaData();
//			ResultSet resultSet = dbm.getTables(null, null, table_name, null);
//			System.out.println("----------");
//			while (resultSet.next()) {
//				String databaseName = resultSet.getString(1);
//				System.out.println(databaseName);
//				if (databaseName.equals(table_name)) {
//					return true;
//				}
//			}
//			resultSet.close();
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return false;
//	}

	public static boolean createTable(String table_name, String variables, String column_list)
			throws ClassNotFoundException {
		String sql = "CREATE TABLE `" + target_table + "` (stt INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
		String[] vari = variables.split(",");
		String[] col = column_list.split(",");
		for (int i = 0; i < vari.length; i++) {
			sql += col[i] + " " + vari[i] + " NOT NULL,";
		}
		sql = sql.substring(0, sql.length() - 1) + ")";
//		System.out.println(sql);
		try {
			PreparedStatement pst = ConnectionDB.getConnection("staging").prepareStatement(sql);
			pst.executeUpdate();
			return true;
		} catch (SQLException e) {
//			e.printStackTrace();
			return false;
		}

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

	public static String readValuesXLSX(File s_file) {
		String values = "";
		String value = "";
		try {
			FileInputStream fileIn = new FileInputStream(s_file);
			XSSFWorkbook workBooks = new XSSFWorkbook(fileIn);
			XSSFSheet sheet = workBooks.getSheetAt(0);
			Iterator<Row> rows = sheet.iterator();
			rows.next();
			int countRow=0;
			while (rows.hasNext()) {
				Row row = rows.next();
				Iterator<Cell> cells = row.cellIterator();
				int count = 0;
				while (count < numOfcol) {
					cells.hasNext();
					try {
						Cell cell = cells.next();
						CellType cellType = cell.getCellType();
						switch (cellType) {
						case NUMERIC:
							if (DateUtil.isCellDateFormatted(cell)) {
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
								value += dateFormat.format(cell.getDateCellValue()) + "|";
							} else {
								value += (long) cell.getNumericCellValue() + "|";
							}
							break;
						case STRING:
								value += cell.getStringCellValue() + "|";
							break;
						case BLANK:
								value +=" " + "|"; 
							break;
						default:
							if (cell.getStringCellValue().toString().equalsIgnoreCase("")) {
								value +=" " + "|"; 
							}
							break;
						}
					} catch (Exception e) {
						value +=" " + "|"; 
					}
					
					
					
					count++;
				}
				if (value.length()>0) {
					values += readLines(value.substring(0, value.length() - 1), "|");
				}
				
//				System.out.println("values:" + values);
				value = "";
				
				
//				if (count != numOfcol) {
//					break;
//				}
			}
			workBooks.close();
			fileIn.close();
			return values.substring(0, values.length() - 1);
		} catch (IOException e) {
			return null;
		}
	}

	private static String readLines(String value, String delim) {
		String values = "";
		StringTokenizer stoken = new StringTokenizer(value, delim);
		if (stoken.countTokens() > 0) {
			stoken.nextToken();
		}
		int countToken = stoken.countTokens();
//		System.out.println(countToken);
		String lines = "(";
		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
//			if (Pattern.matches(NUMBER_REGEX, token)) {
//				lines += (j == countToken - 1) ? token.trim() + ")," : token.trim() + ",";
//			} else {
			lines += (j == countToken - 1) ? "'" + token + "')," : "'" + token + "',";
//			}
			
			values += lines;
			lines = "";
		}
//		System.out.println(values);
		return values;
	}

	public static boolean insertValues(String target_table, String column_list, String values)
			throws ClassNotFoundException {
		String sql = "INSERT INTO `" + target_table + "` (" + column_list + ") VALUES " + values;
		System.out.println(sql);

		try {
			PreparedStatement pst = ConnectionDB.getConnection("staging").prepareStatement(sql);
			pst.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static boolean writeDataToBD(String target_table, String column_list, String values)
			throws ClassNotFoundException {
		if (Staging.insertValues(target_table, column_list, values))
			return true;
		return false;
	}

//	public static void main(String[] args) throws org.apache.poi.openxml4j.exceptions.InvalidFormatException,
//			EncryptedDocumentException, ClassNotFoundException, SQLException, IOException {
//		Staging s = new Staging();
//		s.staging();
//	}
}
