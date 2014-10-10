package io.quantumdb.nemesis.profiler;

import java.io.IOException;
import java.sql.SQLException;

import io.quantumdb.nemesis.operations.NamedOperation;
import io.quantumdb.nemesis.operations.Operations;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Profiler {

	private final ProfilerConfig config;
	private final Database.Type type;
	private final DatabaseCredentials credentials;
	private final int startupTimeout;
	private final int teardownTimeout;

	public Profiler(ProfilerConfig config, Database.Type type, DatabaseCredentials credentials, int startupTimeout, int teardownTimeout) {
		this.config = config;
		this.type = type;
		this.credentials = credentials;
		this.startupTimeout = startupTimeout;
		this.teardownTimeout = teardownTimeout;
	}

	public void profile() throws InterruptedException, IOException, SQLException {
		Session session = new Session(type, config, credentials, startupTimeout, teardownTimeout);
		for (NamedOperation operation : Operations.all()) {
			try {
				session.start(operation);
			}
			finally {
				// Good moment for GC to happen...
				System.gc();
			}
		}
	}

}
