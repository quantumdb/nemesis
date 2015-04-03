package io.quantumdb.nemesis.structure.mysql55;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import io.quantumdb.nemesis.structure.QueryBuilder;
import io.quantumdb.nemesis.structure.Sequence;
import io.quantumdb.nemesis.structure.Table;
import io.quantumdb.nemesis.structure.TableDefinition;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class MysqlDatabase implements Database {

	private Connection connection;

	public void connect(DatabaseCredentials credentials) throws SQLException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.connection = DriverManager.getConnection(credentials.getUrl(),
					credentials.getUsername(), credentials.getPassword());
		}
		catch (ClassNotFoundException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void close() throws SQLException {
		connection.close();
	}

	@Override
	public boolean supports(Feature feature) {
		switch (feature) {
			case COLUMN_CONSTRAINTS:
			case DEFAULT_VALUE_FOR_TEXT:
			case MULTIPLE_AUTO_INCREMENT_COLUMNS:
			case RENAME_INDEX:
				return false;
			default:
				return true;
		}
	}

	@Override
	public List<Table> listTables() throws SQLException {
		String query = "SHOW TABLES";
		List<Table> tables = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			log.debug(query);

			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String tableName = resultSet.getString(1);
				tables.add(new MysqlTable(connection, this, tableName));
			}
		}

		return tables;
	}

	@Override
	public void atomicTableRename(String replacingTableName, String currentTableName, String archivedTableName)
			throws SQLException {

		Savepoint save = null;
		boolean autoCommit = false;
		try {
			autoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			save = connection.setSavepoint();

			String query = "RENAME TABLE %s TO %s, %s TO %s";
			String formatted = String.format(query, currentTableName, archivedTableName, replacingTableName, currentTableName);
			connection.createStatement().execute(formatted);

			connection.commit();
		}
		catch (SQLException e) {
			connection.rollback(save);
			connection.setAutoCommit(autoCommit);
		}
	}

	@Override
	public List<Sequence> listSequences() throws SQLException {
		// MySQL doesn't support sequences...?!?
		return Lists.newArrayList();
	}

	@Override
	public void dropContents() throws SQLException {
		while (!listTables().isEmpty()) {
			for (Table table : listTables()) {
				try {
					table.drop();
				}
				catch (SQLException e) {
					log.warn(e.getMessage(), e);
				}
			}
		}
		for (Sequence sequence : listSequences()) {
			sequence.drop();
		}
	}

	@Override
	public Table createTable(TableDefinition table) throws SQLException {
		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("CREATE TABLE " + table.getName() + " (");

		boolean columnAdded = false;
		for (ColumnDefinition column : table.getColumns()) {
			if (columnAdded) {
				queryBuilder.append(", ");
			}

			queryBuilder.append(column.getName() + " " + column.getType());
			if (column.isIdentity()) {
				queryBuilder.append(" PRIMARY KEY");
			}
			if (!column.isNullable()) {
				queryBuilder.append(" NOT NULL");
			}

			if (column.isAutoIncrement()) {
				queryBuilder.append(" AUTO_INCREMENT");
			}
			else if (!Strings.isNullOrEmpty(column.getDefaultExpression())) {
				queryBuilder.append(" DEFAULT " + column.getDefaultExpression());
			}

			columnAdded = true;
		}

		queryBuilder.append(")");
		execute(queryBuilder.toString());

		return new MysqlTable(connection, this, table.getName());
	}

	void execute(String query) throws SQLException {
		query(query);
		log.debug(query);
	}

	@Override
	public void query(String query) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
		}
		catch (SQLException e) {
			log.error(e.getMessage() + " - " + query, e);
			throw e;
		}
	}

	@Override
	public Connection getConnection() {
		return connection;
	}

}
