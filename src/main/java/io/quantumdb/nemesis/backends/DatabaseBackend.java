package io.quantumdb.nemesis.backends;

import java.sql.SQLException;

import io.quantumdb.nemesis.schema.Column;
import io.quantumdb.nemesis.schema.Table;
import io.quantumdb.nemesis.schema.types.ColumnType;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * This interface offers various simple methods to execute DDL statement for a particular SQL database.
 */
public interface DatabaseBackend {

	/**
	 * Connects to the specified SQL database.
	 *
	 * @param credentials Credentials and information describing to what database to connect.
	 *
	 * @throws SQLException In case no connection to the SQL database could be established.
	 */
	void connect(DatabaseCredentials credentials) throws SQLException, ClassNotFoundException;

	/**
	 * Creates a new table as specified by a Table schema object.
	 *
	 * @param table The Table object describing the required structure of the table.
	 *
	 * @throws SQLException If the table could not be created.
	 */
	void createTable(Table table) throws SQLException;

	/**
	 * Drops an already existing table from the database.
	 *
	 * @param tableName The name of the table to drop.
	 *
	 * @throws SQLException If the table could not be dropped.
	 */
	void dropTable(String tableName) throws SQLException;

	/**
	 * Adds a new column to an already existing table.
	 *
	 * @param tableName The name of the table to add the column to.
	 * @param column    The schema definition of the column.
	 *
	 * @throws SQLException If the column could not be added to the table
	 */
	void addColumn(String tableName, Column column) throws SQLException;

	/**
	 * Drops an already existing column from an existing table.
	 *
	 * @param tableName  The name of the table to drop the column from.
	 * @param columnName The name of the column to drop from the table.
	 *
	 * @throws SQLException If the column could not be dropped from the table.
	 */
	void dropColumn(String tableName, String columnName) throws SQLException;

	/**
	 * Renames an already existing column to something else.
	 *
	 * @param tableName     The name of the table which contains the column to be renamed.
	 * @param columnName    The name of the column to rename.
	 * @param newColumnName The new name of the column.
	 *
	 * @throws SQLException If the column could not be renamed.
	 */
	void renameColumn(String tableName, String columnName, String newColumnName) throws SQLException;

	/**
	 * Creates a one-column index on an already existing column.
	 *
	 * @param tableName  The name of the table which contains the column to index.
	 * @param columnName The name of the column to index.
	 *
	 * @throws SQLException If the column could not be indexed.
	 */
	void createIndex(String tableName, String columnName) throws SQLException;

	/**
	 * Drops an already existing one-column index from the table.
	 *
	 * @param tableName  The name of the table which contains the index.
	 * @param columnName The name of the column on which the index is based.
	 *
	 * @throws SQLException If the index could not be dropped.
	 */
	void dropIndex(String tableName, String columnName) throws SQLException;

	/**
	 * Changes the data type of an already existing column.
	 *
	 * @param tableName  The name of the table which contains the targeted column.
	 * @param columnName The column to change the data type of.
	 * @param type       The new data type of the specified column.
	 *
	 * @throws SQLException If the data type of the column could not be changed.
	 */
	void modifyDataType(String tableName, String columnName, ColumnType type) throws SQLException;

	/**
	 * Sets the default expression for an already existing column.
	 *
	 * @param tableName  The name of the table which contains the targeted column.
	 * @param columnName The name of the column of which you want to change the default expression.
	 * @param expression The new default expression of the column.
	 *
	 * @throws SQLException If the default expression of the targeted column could not be changed.
	 */
	void setDefaultExpression(String tableName, String columnName, String expression) throws SQLException;

	/**
	 * Sets the nullability of an already existing column.
	 *
	 * @param tableName  The name of the table which contains the targeted column.
	 * @param columnName The name of the column for which you want to change the nullability.
	 * @param nullable   True if the column may hold NULL values, or false otherwise.
	 *
	 * @throws SQLException If the nullability of the targeted column could not be changed.
	 */
	void setNullable(String tableName, String columnName, boolean nullable) throws SQLException;

	/**
	 * Adds a foreign key on one or more already existing columns.
	 *
	 * @param tableName             The name of the table which contains the targeted columns.
	 * @param columnNames           The names of the columns which represent the reference.
	 * @param referencedTableName   The name of the table which contains the referenced records.
	 * @param referencedColumnNames The names of the columns to which this foreign key refers.
	 *
	 * @throws SQLException If the foreign key could not be created.
	 */
	void addForeignKey(String tableName, String[] columnNames, String referencedTableName,
			String[] referencedColumnNames) throws SQLException;

	/**
	 * Executes a specified SQL query.
	 *
	 * @param query The SQL query to execute.
	 *
	 * @throws SQLException In case the SQL query could not be executed.
	 */
	void query(String query) throws SQLException;

	/**
	 * Closes the connection to the SQL database.
	 *
	 * @throws SQLException If the connection could not be closed.
	 */
	void close() throws SQLException;

	@AllArgsConstructor(access = AccessLevel.PRIVATE)
	public static enum Type {
		MYSQL("com.mysql.jdbc.Driver"),
		POSTGRES("org.postgresql.Driver");

		@Getter
		private final String driver;
	}

	@NoArgsConstructor(access = AccessLevel.PRIVATE)
	public static final class Factory {

		/**
		 * Creates a new implementation of a DatabaseBackend which can operate on the specified SQL database type.
		 *
		 * @param type The type of SQL database for which you want a DatabaseBackend implementation.
		 *
		 * @return An implementation which can operate on the specified SQL database.
		 */
		public static DatabaseBackend create(Type type) {
			switch (type) {
				case POSTGRES:
					return new PostgresBackend();
				case MYSQL:
					return new MysqlBackend();
				default:
					throw new IllegalArgumentException("Unknown database type: " + type);
			}
		}
	}

}
