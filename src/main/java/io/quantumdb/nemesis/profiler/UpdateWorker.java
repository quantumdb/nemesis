package io.quantumdb.nemesis.profiler;

import java.io.Writer;
import java.sql.SQLException;
import java.util.Random;

import io.quantumdb.nemesis.backends.DatabaseBackend;
import io.quantumdb.nemesis.backends.DatabaseCredentials;


public class UpdateWorker extends Worker {

	private static final String QUERY = "UPDATE %s SET name = 'Dilbert' WHERE id = %d";

	private final Random random;
	private final DatabaseBackend backend;
	private final String tableName;
	
	public UpdateWorker(DatabaseBackend backend, DatabaseCredentials credentials, Writer writer,
			long startingTimestamp, String tableName) {

		super(backend, credentials, writer, startingTimestamp);
		this.backend = backend;
		this.tableName = tableName;
		this.random = new Random();
	}
	
	@Override
	void doAction() throws SQLException {
		int from = random.nextInt(100_000_000);
		backend.query(String.format(QUERY, tableName, from));
	}
	
}
