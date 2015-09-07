package io.quantumdb.nemesis.structure;

import java.sql.SQLException;

public interface Trigger {

	String getName();

	Table getParent();

	void drop() throws SQLException;

}
