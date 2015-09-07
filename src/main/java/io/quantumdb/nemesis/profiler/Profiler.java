package io.quantumdb.nemesis.profiler;

import java.util.List;

import io.quantumdb.nemesis.operations.NamedOperation;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Profiler {

	private final ProfilerConfig config;
	private final Database.Type type;
	private final DatabaseCredentials credentials;
	private final List<NamedOperation> operations;
	private final int startupTimeout;
	private final int teardownTimeout;

	public Profiler(ProfilerConfig config, Database.Type type, DatabaseCredentials credentials, List<NamedOperation> operations, int startupTimeout, int teardownTimeout) {
		this.config = config;
		this.type = type;
		this.credentials = credentials;
		this.operations = operations;
		this.startupTimeout = startupTimeout;
		this.teardownTimeout = teardownTimeout;
	}

	public void profile() throws Exception {
		Session session = new Session(type, config, credentials, startupTimeout, teardownTimeout);
		for (NamedOperation operation : operations) {
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
