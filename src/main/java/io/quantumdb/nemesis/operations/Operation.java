package io.quantumdb.nemesis.operations;

import io.quantumdb.nemesis.structure.Database;

public interface Operation {
	
	default void prepare(Database backend) throws Exception {}

	void perform(Database backend) throws Exception;

	default void cleanup(Database backend) throws Exception {}

	default boolean isSupportedBy(Database backend) {
		return true;
	}

}
