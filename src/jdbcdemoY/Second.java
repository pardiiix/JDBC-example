package jdbcdemoY;
import java.sql.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.LinkedList; 
import java.util.Queue; 

public class Second {
	private Connection conn;
	private static Statement stmt;
	
	
	public void initDatabase(){
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/homework4db?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC", "root", "golabi");
			stmt = conn.createStatement();
			
			//create Database
			String database =  "Create database if not exists homework4db";
			stmt.executeUpdate(database);
			
			//create Person table
	        String Personsql = "CREATE TABLE IF NOT EXISTS Person (\n"
	                + "    pid integer PRIMARY KEY,\n"
	                + "    name varchar(30) NOT NULL,\n"
	                + "    age integer\n"
	                + ");";
			stmt.executeUpdate(Personsql);
			
			
			//create Likes table
			String Likessql = "CREATE TABLE IF NOT EXISTS Likes (\n" + 
					" pid int,\n" + 
					"mid int" +
					",\n" + 
					"FOREIGN KEY(pid) REFERENCES Person(pid),\n" + 
					"FOREIGN KEY(mid) REFERENCES Person(pid)" + 
					");";
			stmt.executeUpdate(Likessql);
			
			
			//read text file
			File txtFile = new File("transfile.txt");
			Scanner sc = new Scanner(txtFile);
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				StringTokenizer st = new StringTokenizer(line);
				String command = (String) st.nextElement();
				
				if(command.equals("1")){
					deletePerson(line);
				}
				else if(command.equals("2")){
					insertPerson(line);
				}
				else if(command.equals("3")) {
					avgAge(line);
					
				}
				else if(command.equals("4")) {
					nameofLikers(line);
				}
				else if(command.equals("5")) {
					AvgAgeofLikers(line);
				}
			}
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
   
    // Transaction 1
    public static void deletePerson(String line) throws SQLException{
    
    	StringTokenizer st = new StringTokenizer(line);
    	st.nextElement();
    	
    	int pid = Integer.parseInt((String) st.nextElement());
    	
    	String check = "Select count(*) from Person where pid=" + pid;
    	ResultSet rs = stmt.executeQuery(check);
    	rs.next();
    	int rowCount = rs.getInt(1);
    	
    	try {
    		if (rowCount != 0) {
    			String firstQuery = "delete from Person where pid=" + pid;
    			stmt.executeUpdate(firstQuery);
//    			System.out.println("#1: Person "+pid+" has been deleted from Person");
    			
    			String secondQuery = "delete from Likes where pid=" + pid;
    			stmt.executeUpdate(secondQuery);
//    			System.out.println("#1: Person "+pid+" has been deleted from Likes");
    			
    			System.out.println("#1: Person "+pid+" has been deleted from both tables");
    		}
    		else
    			System.out.println("#1: Transaction 1 error: No employee exists with pid of " + pid);
    	}
    	catch(SQLException s){
    		System.out.println("There is an error for transaction 1");
    	}
    }
 
    
    
    //Transaction 2
    public void insertPerson(String line) throws SQLException {
       
        StringTokenizer st = new StringTokenizer(line);
        st.nextElement();   
        int pid = Integer.parseInt((String) st.nextElement());
        String name = (String) st.nextElement();
        int age = Integer.parseInt((String) st.nextElement());

       
        String check = "Select count(*) from Person where pid="+pid;
        ResultSet rs = stmt.executeQuery(check);
        rs.next();
        int rowCount = rs.getInt(1);
//        System.out.println(rowCount);
        if(rowCount > 0) {
        	System.out.println("#2: Error! There is a duplicate with pid: " + pid);
        }
        
        String addPerson = "insert into Person Values (" + pid + ", '" + name + "', " + age + ")";
        stmt.executeUpdate(addPerson);
        System.out.println("#2: Person "+ pid + " added to Person table!");
        
        
        //add while has next here
        while(st.hasMoreTokens()) {
        	int nextMid = Integer.parseInt((String) st.nextElement());
        	String addLikes = "insert into Likes Values (" + pid + ", " + nextMid  + ")";
        	stmt.executeUpdate(addLikes);
        	}
    }
    
    
    //Transaction 3 - average
    public void avgAge(String line) throws SQLException{
    	
    	try {
    		ResultSet rs = stmt.executeQuery("select avg(age) from Person");
    		rs.next();
    		int avg = (int) (Float.parseFloat((String)rs.getString(1)));
    		System.out.println("#3: The average age of the people is: " + avg);
    	}
    	catch (SQLException s) {
    		System.out.println("#3: Error! Cannot compute average age");
    	}
    }
       
    
    //Transaction 4
    public void nameofLikers(String line) throws SQLException, NullPointerException{
        //creating an Queue to capture all edges of the graph (edges being pids of the people)
    	Queue<Integer> q = new LinkedList<>();
    	
		StringTokenizer st = new StringTokenizer(line);
		st.nextElement();
        int pid =  Integer.parseInt((String) st.nextElement());
        q.add(pid);
        
        
    	try {         
    		ResultSet rs = stmt.executeQuery("select pid, name from Person where pid in(select mid from Likes where pid=" + pid + ");");
    		while (rs.next()) {
    			  System.out.println("#4! Person "+pid+ " likes Person " + rs.getString(1) + " with the name of: " + rs.getString(2));
    			  q.add(Integer.parseInt((String)rs.getString(1)));
    			}
    		q.remove();

    		//Breadth-first search
    		while (q.isEmpty() == false) {
    			int nextNode = q.peek();
    			ResultSet rs2 = stmt.executeQuery("select pid, name from Person where pid in(select mid from Likes where pid=" + nextNode + ");");
//    			System.out.println(q);
    			q.remove();
    			
    			while (rs2.next()) {
  			    System.out.println("And Person "+ nextNode+ " likes Person " + rs2.getString(1) + " with the name of: " + rs2.getString(2));
  			    q.add(Integer.parseInt((String)rs2.getString(1)));
  			    
    			}
    		}           
    	}
    	catch (SQLException s) {
    		System.out.println("#4: Error! Cannot retrieve names");
    	}
    }
    
    
    //Transaction 5
    public void AvgAgeofLikers(String line) throws SQLException, NullPointerException{
        //creating an Queue to capture all edges of the graph (edges being pids of the people)
    	Queue<Integer> q = new LinkedList<>();
    	Queue<Integer> ageQ = new LinkedList<>();
    	
		StringTokenizer st = new StringTokenizer(line);
		st.nextElement();
        int pid =  Integer.parseInt((String) st.nextElement());
        q.add(pid);
        
        
    	try {         
    		ResultSet rs = stmt.executeQuery("select pid, age from Person where pid in(select mid from Likes where pid=" + pid + ");");
    		while (rs.next()) {
    			  System.out.println("#4! Person "+pid+ " likes Person " + rs.getString(1) + " with the age of: " + rs.getString(2));
    			  q.add(Integer.parseInt((String)rs.getString(2)));
    			  ageQ.add(Integer.parseInt((String)rs.getString(2)));
    			}
    		q.remove();

    		//Breadth-first search
    		while (q.isEmpty() == false) {
    			int nextNode = q.peek();
    			ResultSet rs2 = stmt.executeQuery("select pid, age from Person where pid in(select mid from Likes where pid=" + nextNode + ");");
    			q.remove();
    			
    			while (rs2.next()) {
  			    System.out.println("And Person "+ nextNode+ " likes Person " + rs2.getString(1) + " with the age of: " + rs2.getString(2));
  			    q.add(Integer.parseInt((String)rs2.getString(2)));
  			    ageQ.add(Integer.parseInt((String)rs2.getString(2)));
  			    
    			}
    		System.out.println(ageQ);
    		}           
    	}
    	catch (SQLException s) {
    		System.out.println("#4: Error! Cannot retrieve names");
    	}
    }


    
    
    
	public static void main(String[] args){
		
		Second db = new Second();
		db.initDatabase();
	
	}

}
