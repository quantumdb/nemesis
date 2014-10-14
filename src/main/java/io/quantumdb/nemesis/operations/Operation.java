package io.quantumdb.nemesis.operations;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Database;

public interface Operation {
	
	default void prepare(Database backend) throws SQLException {};

	void perform(Database backend) throws SQLException;
	
	default void cleanup(Database backend) throws SQLException {};

	default boolean isSupportedBy(Database backend) {
		return true;
	}

}
