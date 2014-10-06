package io.quantumdb.nemesis.operations;

import java.sql.SQLException;

import io.quantumdb.nemesis.backends.DatabaseBackend;

public interface Operation {
	
	void prepare(DatabaseBackend backend) throws SQLException;

	void perform(DatabaseBackend backend) throws SQLException;
	
	void cleanup(DatabaseBackend backend) throws SQLException;

}
