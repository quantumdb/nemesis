package io.quantumdb.nemesis.profiler;

import io.quantumdb.nemesis.backends.DatabaseBackend.Type;
import io.quantumdb.nemesis.backends.DatabaseCredentials;
import io.quantumdb.nemesis.operations.NamedOperation;
import io.quantumdb.nemesis.operations.Operations;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Profiler {

	private final ProfilerConfig config;
	private final Type type;
	private final DatabaseCredentials credentials;

	public Profiler(ProfilerConfig config, Type type, DatabaseCredentials credentials) {
		this.config = config;
		this.type = type;
		this.credentials = credentials;
	}

	public void profile() {
		Session session = new Session(type, config, credentials);
		for (NamedOperation operation : Operations.all()) {
			try {
				session.start(operation);
			}
			catch (Throwable e) {
				log.error(e.getMessage(), e);
			}
			finally {
				// Good moment for GC to happen...
				System.gc();
			}
		}
	}

}
