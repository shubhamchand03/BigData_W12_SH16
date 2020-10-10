package general;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ques2 {
	

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

		String load_data = "load data local inpath '/home/cloudera/Downloads/olympic_data.csv' overwrite into table W12olympic";
		try {
			statement.execute(load_data);
			System.out.println("Data is added to the table.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		con.close();

	}

}
