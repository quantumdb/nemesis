package io.quantumdb.nemesis.profiler;


import lombok.Data;

@Data
public class ProfilerConfig {

	private final int readWorkers;
	private final int updateWorkers;
	private final int insertWorkers;
	private final int deleteWorkers;

	public int getTotalWorkers() {
		return  Math.max(0, readWorkers) +
				Math.max(0, updateWorkers) +
				Math.max(0, insertWorkers) +
				Math.max(0, deleteWorkers);
	}
}
