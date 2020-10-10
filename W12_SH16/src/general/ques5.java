package general;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ques5 {
	

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

		String query = "select country, sum(total) from W12olympic"
				+ " group by country";

		ResultSet r = null;
		try {
			r = statement.executeQuery(query);
			ResultSetMetaData meta = r.getMetaData();

			int ncols = meta.getColumnCount();

			System.out.println("Total medals won by each country: ");
			System.out.println("Country, Number of Medals");
			while (r.next()) {
				for (int i = 1; i <= ncols; i++) {
					if (i != ncols)
						System.out.print(r.getString(i) + ", ");
					else
						System.out.print(r.getString(i) + "\n");
				}
			}
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
		statement.close();
		con.close();

	}

}
