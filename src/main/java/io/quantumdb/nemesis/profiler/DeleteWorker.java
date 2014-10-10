package io.quantumdb.nemesis.profiler;

import java.io.Writer;
import java.sql.SQLException;
import java.util.Random;

import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;


public class DeleteWorker extends Worker {

	private static final String QUERY = "DELETE FROM %s WHERE id = %d;";

	private final Database backend;
	private final String tableName;
	private final Random random;

	public DeleteWorker(Database backend, DatabaseCredentials credentials, Writer writer,
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
