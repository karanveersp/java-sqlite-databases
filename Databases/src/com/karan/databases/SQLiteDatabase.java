/**
 * 
 */
package com.karan.databases;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class demonstrates how to use the SQLite database with Java
 * It includes various methods that perform common database operations
 * @author Karan
 */
public class SQLiteDatabase {
	
	
	/**
	 * Connect to database url with given dbName or path
	 * @return Instantiated Connection object
	 */
	private Connection connect(String dbName) {
		String url = "jdbc:sqlite:" + dbName;  // dbName is looked in project folder
		Connection conn = null;
		try {
			// create a connection to the db
			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace(System.out);	
		}
		return conn;
	}
	
	/**
	 * Create a database with given fileName
	 * @param fileName
	 */
	public void createNewDatabase(String dbName) {
		// connects to given database - relative path points to inside project folder
		try {
			Connection conn = this.connect(dbName);						// open connection
			if (conn != null) {
				DatabaseMetaData meta = conn.getMetaData();
				System.out.println("The driver name is " + meta.getDriverName());
				System.out.println("A new database has been created.");
			}
			conn.close(); 												// close connection
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}


	/**
	 * Create a new table in the test database
	 */
	public void createNewTable(String dbName, String tableName) {
		// SQL statement for creating a new table
		String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n"
				+ "id integer PRIMARY KEY,\n"
				+ "first_name VARCHAR(20) NOT NULL,\n"
				+ "last_name VARCHAR(20) NOT NULL,\n"
				+ "manager_id integer NOT NULL,\n"
				+ "join_date DATE NOT NULL,\n"
				+ "billable_hours double NOT NULL);";
		
		try {
			Connection conn = this.connect(dbName);			// open connection
			
			Statement stmt = conn.createStatement();
			
			// create a new table using prepared sql statement
			stmt.execute(sql);
			System.out.println("Executed create table statement");
			
			conn.close();									// close connection
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * Execute SELECT * statment
	 * @param dbName Name of the database to connect to
	 * @param tableName Name of table to select from
	 */
	public void selectAll(String dbName, String tableName) {
		String sql = "SELECT * FROM " + tableName;
		
		try {
			Connection conn = this.connect(dbName);					// open connection
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			int numColumns = rs.getMetaData().getColumnCount(); // get column count to use later
			
			// loop through the result set
			while (rs.next()) {
				
//				System.out.println(rs.getInt("id") + " | "
//						+ rs.getString("first_name") + " | "
//						+ rs.getString("last_name") + " | "
//						+ rs.getInt("manager_id") + " | "
//						+ rs.getString("join_date") + " | "
//						+ rs.getDouble("billable_hours"));
				
				// generalized loop to print all attributes for each row
				for (int i = 1; i <= numColumns; i++) {
					if (i != numColumns)
						System.out.print(rs.getString(i) + " | ");
					else
						System.out.print(rs.getString(i) + "\n");
				}
			}
			
			conn.close();											// close connection
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}
	}
	
	/**
	 * Insert given values into the given table in given database
	 * @param dbName	Name of db to use
	 * @param tableName Name of table in db
	 * @param values	Values to insert
	 */
	public void inserInto(String dbName, String tableName, String values) {
		String sql = "INSERT INTO " + tableName + " VALUES (" + values + ");";
		try {
			Connection conn = this.connect(dbName);						// open connection
			Statement stmt = conn.createStatement();
			
			stmt.execute(sql);
			System.out.println("Inserted value (" + values + ")");
			
			conn.close();												// close connection
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}
	
	
	/**
	 * Delete row with given value of the given table in given database
	 * @param dbName	Name of db to use
	 * @param tableName Name of table in db
	 * @param values	Value to delete
	 */
	public void deleteFrom(String dbName, String tableName, String values) {
		String sql = "DELETE FROM " + tableName + " WHERE (" + values + ");";
		try {
			Connection conn = this.connect(dbName);						// open connection
			Statement stmt = conn.createStatement();
			
			stmt.execute(sql);
			System.out.println("Deleted values where " + values);
			
			conn.close();												// close connection
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}
	
	
	


	public static void main(String[] args) {
		SQLiteDatabase sqldb = new SQLiteDatabase();
		sqldb.createNewTable("test.db", "employees");
		sqldb.selectAll("test.db", "employees");
		sqldb.createNewDatabase("coolHandLuke.db");
		sqldb.createNewTable("coolHandLuke.db", "prisoners");
		sqldb.deleteFrom("coolHandLuke.db", "prisoners", "id=\"1\"");
		sqldb.selectAll("coolHandLuke.db", "prisoners");
		sqldb.inserInto("coolHandLuke.db", "prisoners", "1, \"Luke\", \"Luke\", 0, \"3/18/1967\", 200");
		sqldb.selectAll("coolHandLuke.db", "prisoners");
		
		/* If methods were static, you'd use the following calls */
//		connect();
//		createNewDatabase("test2.db");
//		createNewTable();
//		selectAll("employees");
		
	}

}
