package io.quantumdb.nemesis.profiler;

import java.io.Writer;
import java.sql.SQLException;

import io.quantumdb.nemesis.backends.DatabaseBackend;
import io.quantumdb.nemesis.backends.DatabaseCredentials;
import io.quantumdb.nemesis.backends.RandomNameGenerator;


public class InsertWorker extends Worker {

	private static final String QUERY = "INSERT INTO %s (name) VALUES ('%s');";

	private final DatabaseBackend backend;
	private final String tableName;

	public InsertWorker(DatabaseBackend backend, DatabaseCredentials credentials, Writer writer,
			long startingTimestamp, String tableName) {

		super(backend, credentials, writer, startingTimestamp);
		this.backend = backend;
		this.tableName = tableName;
	}
	
	@Override
	void doAction() throws SQLException {
		backend.query(String.format(QUERY, tableName, RandomNameGenerator.generate()));
	}
	
}
