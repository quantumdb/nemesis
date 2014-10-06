package io.quantumdb.nemesis.operations;

import java.sql.SQLException;

import io.quantumdb.nemesis.backends.DatabaseBackend;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

@Data
public class NamedOperation implements Operation {

	private final String name;
	
	@Getter(AccessLevel.NONE)
	private final Operation operation;
	
	@Override
	public void perform(DatabaseBackend backend) throws SQLException {
		operation.perform(backend);
	}
	
	@Override
	public void prepare(DatabaseBackend backend) throws SQLException {
		operation.prepare(backend);
	}
	
	@Override
	public void cleanup(DatabaseBackend backend) throws SQLException {
		operation.cleanup(backend);
	}
	
}
