package general;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ques4 {
	

private static String driverClass = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args)throws SQLException  {
		// TODO Auto-generated method stub
		
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException e) {
			System.out.println(e.toString());
			System.exit(1);
		}

		Connection con = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/default", "hive", "");

		Statement statement = con.createStatement();

		String createBucket = "create table bcountry ("
				+ "athlete_name string, age int, country string, year int, closing_date string, sport string, gold int, silver int, bronze int, total int"
				+ ") clustered by (country) into 10 buckets"
				+ " row format delimited" + " fields terminated by ','";

		try {
			statement.execute(createBucket);
			System.out.println("Bucket is created.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		try {
			statement.execute("insert overwrite table bcountry select * from W12olympic");
			System.out.println("Data is loaded successfully");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
		
		statement.close();
		con.close();

	}

}
