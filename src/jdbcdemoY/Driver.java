package jdbcdemoY;

import java.sql.*;

public class Driver {
	private static String CreateTablePerson = "create table testdb.person" + 
			"(pid integer not null, " + 
			"name varchar(20), "+
			"age integer," +
			"PRIMARY KEY (pid)";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String username = "root";
			String password= "golabi";
			String serverParam = "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
			//1. get a connection to the database
			Connection myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/testdb" + serverParam, username, password);
			
			//2. create a statement
			Statement myStmt = myConn.createStatement();
			//3. execute SQL query
 
			myStmt.executeUpdate(CreateTablePerson);
			System.out.println("Person table created");
			//4. process the result set
//			while (myRs.next()) {
//				System.out.println(myRs.getString("pid") + "," + myRs.getString("name"));
//			}
//		
		}//end of try
		catch (Exception exc){
			exc.printStackTrace();
		}//end of catch
	}

}
