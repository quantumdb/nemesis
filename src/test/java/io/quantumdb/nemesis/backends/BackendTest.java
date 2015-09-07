package io.quantumdb.nemesis.backends;

import java.util.Arrays;
import java.util.Collection;

import com.google.common.base.Strings;
import io.quantumdb.nemesis.operations.DefaultOperations;
import io.quantumdb.nemesis.profiler.DatabaseStructure;
import io.quantumdb.nemesis.profiler.Profiler;
import io.quantumdb.nemesis.profiler.ProfilerConfig;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.Database.Type;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Slf4j
@RunWith(Parameterized.class)
public class BackendTest {

	@Parameterized.Parameters(name = "{index}: type={0}")
	public static Collection<?> getBackends() {
		return Arrays.asList(new Object[][] {
				{ Type.POSTGRESQL, new DatabaseCredentials("jdbc:postgresql://localhost", "profiler",
						get("PG_USER", "profiler"), get("PG_PASSWORD", "profiler"))},
				{ Type.MYSQL_55, new DatabaseCredentials("jdbc:mysql://localhost", "nemesis",
						get("MYSQL_USER", "root"), get("MYSQL_PASSWORD", "root")) },
				{ Type.MYSQL_56, new DatabaseCredentials("jdbc:mysql://localhost", "nemesis",
						get("MYSQL_USER", "root"), get("MYSQL_PASSWORD", "root")) }
		});
	}

	private static String get(String envKey, String defaultValue) {
		String envValue = System.getenv(envKey);
		if (Strings.isNullOrEmpty(envValue)) {
			return defaultValue;
		}
		return envValue;
	}

	private final Database.Type type;
	private final DatabaseCredentials credentials;

	public BackendTest(Database.Type type, DatabaseCredentials credentials) {
		this.type = type;
		this.credentials = credentials;
	}

	@Test
	public void runProfiler() throws Exception {
		DatabaseStructure preparer = new DatabaseStructure(type, credentials);
		preparer.dropStructure();
		preparer.prepareStructureAndRows(10);

		ProfilerConfig config = new ProfilerConfig(1, 1, 1, 1);
		Profiler profiler = new Profiler(config, type, credentials, new DefaultOperations().all(), 50, 50);

		try {
			profiler.profile();
		}
		catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		}
		finally {
			preparer.dropStructure();
		}
	}

}
