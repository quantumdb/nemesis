package io.quantumdb.nemesis.structure;

import java.sql.SQLException;

public interface Index {

	String getName();

	Table getParent();

	boolean isUnique();

	boolean isPrimary();

	void rename(String name) throws SQLException;

	void drop() throws SQLException;

}
