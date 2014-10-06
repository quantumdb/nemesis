package io.quantumdb.nemesis.backends;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.base.Joiner;
import io.quantumdb.nemesis.schema.Column;
import io.quantumdb.nemesis.schema.Table;
import io.quantumdb.nemesis.schema.types.ColumnType;

public class PostgresBackend implements DatabaseBackend {

	private Connection connection;
	private Statement stmt;
	
	@Override
	public void connect(DatabaseCredentials credentials) throws SQLException, ClassNotFoundException {
		this.connection = DatabaseConnector.connect(Type.POSTGRES, credentials);
		this.connection.setAutoCommit(true);
		this.stmt = connection.createStatement();
	}

	@Override
	public void createTable(Table table) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE " + table.getName() + " (");

		for (int i = 0; i < table.getColumns().size(); i++) {
			if (i > 0) {
				builder.append(", ");
			}

			Column column = table.getColumns().get(i);
			builder.append(column.getName() + " " + getColumnType(column));

			if (column.isUnsigned()) {
				throw new IllegalArgumentException("PostgreSQL does not support unsigned data types");
			}
			if (column.isPrimaryKey()) {
				builder.append(" PRIMARY KEY");
			}
			if (column.isNotNull()) {
				builder.append(" NOT NULL");
			}
			if (column.getDefaultValueExpression() != null) {
				builder.append(" DEFAULT " + column.getDefaultValueExpression());
			}
		}

		builder.append(");");
		stmt.execute(builder.toString());

		for (Column column : table.getColumns()) {
			if (column.isAutoIncrement()) {
				stmt.execute("CREATE SEQUENCE " + table.getName() + "_" + column.getName() + "_seq;");
				setDefaultExpression(table.getName(), column.getName(), "NEXTVAL('" + table.getName() + "_" + column.getName() + "_seq')");
			}
		}
	}

	@Override
	public void dropTable(String tableName) throws SQLException {
		stmt.execute("DROP TABLE " + tableName);
		stmt.execute("DROP SEQUENCE " + tableName + "_id_seq");
	}

	@Override
	public void addColumn(String tableName, Column column) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append("ALTER TABLE " + tableName + " ADD COLUMN ");
		builder.append(column.getName() + " " + getColumnType(column));
		
		if (column.isUnsigned()) {
			throw new IllegalArgumentException("PostgreSQL does not support unsigned data types");
		}
		if (column.isPrimaryKey()) {
			builder.append(" PRIMARY KEY");
		}
		if (column.isNotNull()) {
			builder.append(" NOT NULL");
		}
		if (column.isAutoIncrement()) {
			builder.append(" DEFAULT NEXTVAL('" + tableName + "_" + column.getName() + "_seq')");
		}
		else if (column.getDefaultValueExpression() != null) {
			builder.append(" DEFAULT " + column.getDefaultValueExpression());
		}
		
		if (column.isAutoIncrement()) {
			stmt.execute("CREATE SEQUENCE " + tableName + "_" + column.getName() + "_seq;");
		}
		
		stmt.execute(builder.toString() + ";");
		
		if (column.isAutoIncrement()) {
			stmt.execute("ALTER SEQUENCE " + tableName + "_" + column.getName() + "_seq" 
						+ " OWNED BY " + tableName + "." + column.getName() + ";");
		}
	}

	@Override
	public void dropColumn(String tableName, String columnName) throws SQLException {
		stmt.execute("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS " + columnName + ";");
	}

	@Override
	public void renameColumn(String tableName, String columnName, String newColumnName) throws SQLException {
		stmt.execute("ALTER TABLE " + tableName + " RENAME COLUMN " + columnName + " TO " + newColumnName);
	}

	@Override
	public void createIndex(String tableName, String columnName) throws SQLException {
		stmt.execute("CREATE INDEX CONCURRENTLY " + tableName + "_idx_" + columnName + " ON " + tableName + " (" + columnName + ");");
	}

	@Override
	public void dropIndex(String tableName, String columnName) throws SQLException {
		stmt.execute("DROP INDEX CONCURRENTLY IF EXISTS " + tableName + "_idx_" + columnName);
	}

	@Override
	public void modifyDataType(String tableName, String columnName, ColumnType type) throws SQLException {
		stmt.execute("ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " TYPE " + getColumnType(type));
	}

	@Override
	public void setDefaultExpression(String tableName, String columnName, String expression) throws SQLException {
		stmt.execute("ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " SET DEFAULT " + expression);
	}

	@Override
	public void setNullable(String tableName, String columnName, boolean nullable) throws SQLException {
		if (nullable) {
			stmt.execute("ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " DROP NOT NULL");
		}
		else {
			stmt.execute("ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " SET NOT NULL");
		}
	}

	@Override
	public void addForeignKey(String tableName, String[] columnNames,
			String referencedTableName, String[] referencedColumnNames) throws SQLException {

		String forKeyName = tableName + "_" + Joiner.on("-").join(columnNames) + "_idx_"
				+ referencedTableName + "_" + Joiner.on("-").join(referencedColumnNames);

		stmt.execute("ALTER TABLE " + tableName + " ADD CONSTRAINT " + forKeyName
				+ " FOREIGN KEY (" + Joiner.on(",").join(columnNames) + ")"
				+ " REFERENCES " + referencedTableName + " (" + Joiner.on(",").join(referencedColumnNames) + ");");
	}

	@Override
	public void query(String query) throws SQLException {
		stmt.execute(query);
	}

	private String getColumnType(Column column) {
		ColumnType columnType = column.getType();
		return getColumnType(columnType);
	}

	private String getColumnType(ColumnType columnType) {
		switch (columnType.getType()) {
			case INT1:
			case INT2:
				return "smallint";
			case INT4:
				return "integer";
			case INT8:
				return "bigint";
			case VARCHAR:
				return "varchar(" + columnType.getLength() + ")";
			case TEXT:
				return "text";
			default:
				throw new IllegalArgumentException("Type: " + columnType.getType() + " is not supported");
		}
	}

	@Override
	public void close() throws SQLException {
		connection.close();
	}

}
