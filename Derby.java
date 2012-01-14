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
import java.util.Random;

public class Derby {

	private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private String protocol = "jdbc:derby:";

	private Connection connection = null;
	private Statement statement = null;
	
	private String select = null;
	private String join = null;
	private String where = null;
	
	HashMap<String, String> tables = new HashMap<String, String>();

	public Derby(String databaseName, String connectionOptions) {
		
		databaseName = databaseName.toLowerCase().trim();
		
		this.loadDriver();
		
		try {

			this.connection = DriverManager.getConnection(
				new StringBuilder(this.protocol).append(databaseName).append(connectionOptions).toString()
			);

		} catch (SQLException exception) {

			System.err.println(
				new StringBuilder("\nCould not connect to database ").append(databaseName).toString()
			);

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

			System.err.println(
				new StringBuilder("\nUnable to load the JDBC driver ").append(driver).toString()
			);

			System.err.println("Please check your CLASSPATH.");

		} catch (InstantiationException exception) {

			System.err.println(
				new StringBuilder("\nUnable to instantiate the JDBC driver ").append(driver).toString()
			);

		} catch (IllegalAccessException exception) {

			System.err.println(
				new StringBuilder("\nNot allowed to access the JDBC driver ").append(driver).toString()
			);

		}
	}
	
	private String lowerTrim(String string) {
		
		return string.toLowerCase().trim();
		
	}
	
	public void createTable(String tableName, String columns) {
		
		tableName = this.lowerTrim(tableName);
		
		columns = this.lowerTrim(columns);
		
		this.executeDDL(
			new StringBuilder("CREATE TABLE ").append(tableName).append(" (").append(columns).append(")").toString()
		);
		
	}
	
	public void insert(String tableName, String columns, String values) {
		
		tableName = this.lowerTrim(tableName);
		
		columns = this.lowerTrim(columns);
		
		values = this.lowerTrim(values);
		
		this.executeDDL(
			new StringBuilder("INSERT INTO ").append(tableName).append(" (").append(columns).append(") ").append("VALUES (").append(values).append(")").toString()
		);
		
	}

	private void executeDDL(String query) {

		try {

			this.statement = this.connection.createStatement();

			this.statement.executeUpdate(query);

			this.statement.close();

		} catch (SQLException exception) {

			exception.printStackTrace();

		}

	}
	
	public List<HashMap<String, String>> get(String tableName) {
		
		tableName = this.lowerTrim(tableName);
		
		String select = this.getSelect();
		
		String join = this.getJoin();
		
		String where = this.getWhere();
		
		return this.executeDML(
			new StringBuilder("SELECT ").append(select).append(" FROM ").append(tableName).append(join).append(where).toString()
		);
		
	}
	
	public List<HashMap<String, String>> get(String tableName, int limit) {
		
		tableName = this.lowerTrim(tableName);
		
		String select = this.getSelect();
		
		return this.executeDML(
			new StringBuilder("SELECT * FROM (SELECT ROW_NUMBER() OVER() AS rownum, ").append(select).append(" FROM ").append(tableName).append(join).append(where).append(") AS tmp WHERE rownum <= ").append(limit).toString()
		);
		
	}
	
	public List<HashMap<String, String>> get(String tableName, int limit, int offset) {
		
		tableName = this.lowerTrim(tableName);
		
		String select = this.getSelect();
		
		return this.executeDML(
			new StringBuilder("SELECT * FROM (SELECT ROW_NUMBER() OVER() AS rownum, ").append(select).append(" FROM ").append(tableName).append(join).append(where).append(") AS tmp WHERE rownum > ").append(offset).append(" AND rownum <= ").append(offset + limit).toString()
		);
		
	}
	
	public void select(String columns) {
		
		StringBuilder selectStatement;
		
		if(this.select != null)
			
			selectStatement = new StringBuilder(this.select).append(", ");
		
		else
			
			selectStatement = new StringBuilder();
		
		String[] columnsArray = columns.split(",");
		
		for(int i = 0; i < columnsArray.length; i++) {
		
			columnsArray[i] = new StringBuilder().append(
				this.lowerTrim(columnsArray[i].replace("`", ""))
			).toString();
			
			selectStatement.append(columnsArray[i]).append(", ");
			
		}
		
		this.select = selectStatement.substring(0, selectStatement.lastIndexOf(","));
		
	}
	
	public void selectMax(String column, String alias) {
		
		column = this.lowerTrim(column);
		
		alias = this.lowerTrim(alias);
		
		if(this.select != null)
			
			this.select = new StringBuilder(this.select).append(", MAX(").append(column).append(") AS ").append(alias).toString();
		
		else
			
			this.select = new StringBuilder("MAX(").append(column).append(") AS ").append(alias).toString();
		
	}
	
	public void selectMin(String column, String alias) {
		
		column = this.lowerTrim(column);
		
		alias = this.lowerTrim(alias);
		
		if(this.select != null)
			
			this.select = new StringBuilder(this.select).append(", MIN(").append(column).append(") AS ").append(alias).toString();
		
		else
			
			this.select = new StringBuilder("MIN(").append(column).append(") AS ").append(alias).toString();
		
	}
	
	public void selectAvg(String column, String alias) {
		
		column = this.lowerTrim(column);
		
		alias = this.lowerTrim(alias);
		
		if(this.select != null)
			
			this.select = new StringBuilder(this.select).append(", AVG(CAST(").append(column).append(" AS DOUBLE)) AS ").append(alias).toString();
		
		else
			
			this.select = new StringBuilder("AVG(CAST(").append(column).append(" AS DOUBLE)) AS ").append(alias).toString();
		
	}
	
	public void selectSum(String column, String alias) {
		
		column = this.lowerTrim(column);
		
		alias = this.lowerTrim(alias);
		
		if(this.select != null)
			
			this.select = new StringBuilder(this.select).append(", SUM(").append(column).append(") AS ").append(alias).toString();
		
		else
			
			this.select = new StringBuilder("SUM(").append(column).append(") AS ").append(alias).toString();
		
	}
	
	public void join(String column, String condition, String type) {
		
		type = type.toUpperCase().trim();
		
		column = this.lowerTrim(column);
		
		condition = this.lowerTrim(condition);
		
		if(this.join != null)
			
			this.join = new StringBuilder(this.join).append(" ").append(type).append(" JOIN ").append(column).append(" ON (").append(condition).append(")").toString();
		
		else
			
			this.join = new StringBuilder(" ").append(type).append(" JOIN ").append(column).append(" ON (").append(condition).append(")").toString();
		
	}
	
	public void where(String condition) {
		
		condition = this.lowerTrim(condition);
		
		if(this.where != null)
			
			this.where = new StringBuilder(this.where).append(" AND ").append(condition).toString();
		
		else
			
			this.where = new StringBuilder(" WHERE ").append(condition).toString();
		
	}
	
	public void or_where(String condition) {
		
		condition = this.lowerTrim(condition);
		
		if(this.where != null)
			
			this.where = new StringBuilder(this.where).append(" OR ").append(condition).toString();
		
		else
			
			this.where = new StringBuilder(" WHERE ").append(condition).toString();
		
	}
	
	private String getSelect() {
		
		String select;
		
		if(this.select != null)
			
			select = this.select;
		
		else
			
			select = "*";
		
		this.select = null;
		
		return select;
		
	}
	
	private String getJoin() {
		
		String join;
		
		if(this.join != null)
			
			join = this.join;
		
		else
			
			join = "";
		
		this.join = null;
		
		return join;
		
	}
	
	private String getWhere() {
		
		String where;
		
		if(this.where != null)
			
			where = this.where;
		
		else
			
			where = "";
		
		this.where = null;
		
		return where;
		
	}
		
	private List<HashMap<String, String>> executeDML(String query) {
		
		System.out.println(query);

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
			
			this.select = null;
			
			if(resultList.size() < 1)
				
				resultList = null;
			
		} catch (SQLException exception) {

			exception.printStackTrace();

			resultList = null;

		}

		return resultList;

	}

	public boolean tableExists(String tableNamePattern) {
		
		tableNamePattern = tableNamePattern.toUpperCase().trim();

		boolean exists = false;

		try {

			DatabaseMetaData metaData = this.connection.getMetaData();

			ResultSet result = metaData.getTables(null, "APP", tableNamePattern, null);

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

			DriverManager.getConnection(
				new StringBuilder(this.protocol).append(";shutdown=true").toString()
			);

		} catch (SQLException exception) {

			if (exception.getErrorCode() == 50000 && exception.getSQLState().equals("XJ015")) {

				// we got the expected exception

			} else {

				System.err.println("Derby did not shut down normally");

			}
		}		
	}
}
