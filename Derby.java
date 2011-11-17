import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Derby {

	private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocol = "jdbc:derby:";

	private Connection connection = null;
	private Statement statement = null;

	public Derby(String dbName) {

		this.loadDriver();

		try {

			this.connection = DriverManager.getConnection(this.protocol + dbName + ";create=true");

		} catch (SQLException exception) {

			System.err.println("\nCould not connect to database '" + dbName + "'");

		}

	}

	private void loadDriver() {
		
		/*
		 *  The JDBC driver is loaded by loading its class.
		 *  If you are using JDBC 4.0 (Java SE 6) or newer, JDBC drivers may
		 *  be automatically loaded, making this code optional.
		 *
		 *  In an embedded environment, this will also start up the Derby
		 *  engine (though not any databases), since it is not already
		 *  running. In a client environment, the Derby engine is being run
		 *  by the network server framework.
		 *
		 *  In an embedded environment, any static Derby system properties
		 *  must be set before loading the driver to take effect.
		 */
		
		try {
			
			Class.forName(this.driver).newInstance();
			
		} catch (ClassNotFoundException exception) {
			
			System.err.println("\nUnable to load the JDBC driver " + driver);
			
			System.err.println("Please check your CLASSPATH.");
			
		} catch (InstantiationException exception) {
			
			System.err.println("\nUnable to instantiate the JDBC driver " + driver);

		} catch (IllegalAccessException exception) {

			System.err.println("\nNot allowed to access the JDBC driver " + driver);

		}
	}
	
	public void executeDDL(String query) {

		try {

			this.statement = this.connection.createStatement();

			this.statement.executeUpdate(query);

			this.statement.close();

		} catch (SQLException exception) {

			exception.printStackTrace();

		}

	}

	public List<HashMap<String, String>> executeDML(String query) {

		List<HashMap<String, String>> resultList = new LinkedList<HashMap<String, String>>();

		try {

			this.statement = this.connection.createStatement();

			ResultSet result = this.statement.executeQuery(query);			

			if(result.next()) {

				ResultSetMetaData columns = result.getMetaData();

				do {

					HashMap<String, String> row = new HashMap<String, String>();

					for(int index = 0; index < columns.getColumnCount(); index++)

						row.put(columns.getColumnName(index + 1).toLowerCase(), result.getString(index + 1));

					resultList.add(row);

				} while(result.next());

			}

			result.close();

			this.statement.close();

		} catch (SQLException exception) {

			exception.printStackTrace();

			resultList = null;

		}

		return resultList;

	}

	public boolean tableExists(String tableNamePattern) {

		boolean exists = false;

		try {

			DatabaseMetaData metaData = this.connection.getMetaData();

			ResultSet result = metaData.getTables(null, "APP", tableNamePattern.toUpperCase(), null);

			if(result.next())

				exists = true;

			result.close();

		} catch (SQLException exception) {

			exception.printStackTrace();

		}

		return exists;

	}
	
	public void close() {
		
		try {

			this.connection.close();

			DriverManager.getConnection(this.protocol + ";shutdown=true");

		} catch (SQLException exception) {

			if (exception.getErrorCode() == 50000 && "XJ015".equals(exception.getSQLState())) {

				// we got the expected exception

			} else {

				System.err.println("Derby did not shut down normally");

			}
		}		
	}
}
