package io.quantumdb.nemesis.structure.postgresql;

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
public class PostgresDatabase implements Database {

	private Connection connection;
	private DatabaseCredentials credentials;

	public void connect(DatabaseCredentials credentials) throws SQLException {
		try {
			Class.forName("org.postgresql.Driver");
			this.connection = DriverManager.getConnection(credentials.getUrl() + "/" + credentials.getDatabase(),
					credentials.getUsername(), credentials.getPassword());
			this.credentials = credentials;
		}
		catch (ClassNotFoundException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public DatabaseCredentials getCredentials() {
		return credentials;
	}

	@Override
	public void close() throws SQLException {
		connection.close();
	}

	@Override
	public boolean supports(Feature feature) {
		// PostgreSQL is just awesome...
		return true;
	}

	@Override
	public List<Table> listTables() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT DISTINCT(table_name) AS table_name ")
				.append("FROM information_schema.columns ")
				.append("WHERE table_schema = ? ")
				.append("ORDER BY table_name ASC")
				.toString();

		List<Table> tables = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, "public");

			log.debug(query);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String tableName = resultSet.getString("table_name");
				tables.add(new PostgresTable(connection, this, tableName));
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

			String query = "ALTER TABLE %s RENAME TO %s";
			connection.createStatement().execute(String.format(query, currentTableName, archivedTableName));
			connection.createStatement().execute(String.format(query, replacingTableName, currentTableName));

			connection.commit();
		}
		catch (SQLException e) {
			connection.rollback(save);
			connection.setAutoCommit(autoCommit);
		}
	}

	@Override
	public List<Sequence> listSequences() throws SQLException {
		List<Sequence> sequences = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT c.relname AS name FROM pg_class c WHERE c.relkind = 'S';");

			while (resultSet.next()) {
				String name = resultSet.getString("name");
				sequences.add(new PostgresSequence(this, name));
			}
		}
		return sequences;
	}

	@Override
	public void dropContents() throws SQLException {
		for (Table table : listTables()) {
			table.drop();
		}
		for (Sequence sequence : listSequences()) {
			sequence.drop();
		}
	}

	@Override
	public Database getSetupDelegate() {
		return this;
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
				queryBuilder.append(" DEFAULT NEXTVAL('" + table.getName() + "_" + column.getName() + "_seq')");

				execute("CREATE SEQUENCE " + table.getName() + "_" + column.getName() + "_seq;");
			}
			else if (!Strings.isNullOrEmpty(column.getDefaultExpression())) {
				queryBuilder.append(" DEFAULT " + column.getDefaultExpression());
			}

			columnAdded = true;
		}

		queryBuilder.append(")");
		execute(queryBuilder.toString());

		for (ColumnDefinition column : table.getColumns()) {
			if (column.isAutoIncrement()) {
				execute("ALTER SEQUENCE " + table.getName() + "_" + column.getName() + "_seq"
						+ " OWNED BY " + table.getName() + "." + column.getName() + ";");
			}
		}

		return new PostgresTable(connection, this, table.getName());
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
