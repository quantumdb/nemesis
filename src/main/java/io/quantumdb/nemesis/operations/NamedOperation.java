package io.quantumdb.nemesis.operations;

import io.quantumdb.nemesis.structure.Database;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

@Data
public class NamedOperation implements Operation {

	private final String name;
	
	@Getter(AccessLevel.NONE)
	private final Operation operation;
	
	@Override
	public void perform(Database backend) throws Exception {
		operation.perform(backend);
	}
	
	@Override
	public void prepare(Database backend) throws Exception {
		operation.prepare(backend);
	}

	@Override
	public void cleanup(Database backend) throws Exception {
		operation.cleanup(backend);
	}

	@Override
	public boolean isSupportedBy(Database backend) {
		return operation.isSupportedBy(backend);
	}

}
