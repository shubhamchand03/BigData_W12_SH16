package general;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ques3 {
	

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

		String partitions= "create table pmedal ("
				+ "athlete_name string, age int, country string, closing_date string, sport string, gold int, silver int, bronze int, total int"
				+ ") partitioned by (year string)" + " row format delimited"
				+ " fields terminated by ','";
		try {
			statement.execute(partitions);
			System.out.println("pmedal partition is created.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		try {
			statement.execute("set hive.exec.dynamic.partition.mode=nonstrict");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		String loadPartition = "insert overwrite table pmedal partition(year)"
				+ " select athlete_name, age, country, cast(year as string), closing_date, sport, gold, silver, bronze, total from W12olympic";
		try {
			statement.execute(loadPartition);
			System.out.println("Data is loaded.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
		
		statement.close();
		con.close();

	}

}
