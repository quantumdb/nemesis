package io.quantumdb.nemesis.structure.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

	public void connect(DatabaseCredentials credentials) throws SQLException {
		try {
			Class.forName("org.postgresql.Driver");
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
	public List<Sequence> listSequences() throws SQLException {
		return PostgresSequence.listSequences(connection, this);
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

	@Override
	public String toString() {
		return "PostgreSQL";
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
