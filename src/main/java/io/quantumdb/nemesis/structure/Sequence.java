package io.quantumdb.nemesis.structure;

import java.sql.SQLException;

public interface Sequence {

	String getName();

	Database getParent();

	void drop() throws SQLException;

}
