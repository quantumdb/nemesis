package io.quantumdb.nemesis.structure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import io.quantumdb.nemesis.structure.postgresql.PostgresDatabase;

public interface Database {

	public enum Type {
		POSTGRESQL {
			@Override
			public Database createBackend() {
				return new PostgresDatabase();
			}
		};

		public abstract Database createBackend();
	}

	void connect(DatabaseCredentials credentials) throws SQLException;
	void close() throws SQLException;

	Table createTable(TableDefinition table) throws SQLException;

	List<Table> listTables() throws SQLException;

	default Table getTable(String name) throws SQLException {
		for (Table table : listTables()) {
			if (table.getName().equals(name)) {
				return table;
			}
		}
		throw new SQLException("No table exists with name: " + name);
	}

	default boolean hasTable(String name) throws SQLException {
		for (Table table : listTables()) {
			if (table.getName().equals(name)) {
				return true;
			}
		}
		return false;
	}

	List<Sequence> listSequences() throws SQLException;

	void dropContents() throws SQLException;

	void query(String query) throws SQLException;
	Connection getConnection();

}
