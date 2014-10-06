package io.quantumdb.nemesis.profiler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.backends.DatabaseBackend;
import io.quantumdb.nemesis.backends.DatabaseBackend.Type;
import io.quantumdb.nemesis.backends.DatabaseCredentials;
import io.quantumdb.nemesis.operations.NamedOperation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Session {

	private final Type type;
	private final ProfilerConfig config;
	private final DatabaseCredentials credentials;

	public Session(Type type, ProfilerConfig config, DatabaseCredentials credentials) {
		this.type = type;
		this.config = config;
		this.credentials = credentials;
	}
	
	public File start(NamedOperation operation) throws IOException, InterruptedException, SQLException,
			ClassNotFoundException {

		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(config.getTotalWorkers());
		
		sleep(1000);

		DatabaseBackend backend = DatabaseBackend.Factory.create(type);
		backend.connect(credentials);
		operation.prepare(backend);
		
		sleep(1000);

		File folder = new File("logs/" + type + "/" + operation.getName() + "/");
		folder.mkdirs();

		long start = System.currentTimeMillis();

		List<Writer> writers = Lists.newArrayList();
		List<Worker> workers = Lists.newArrayList();

		Writer opWriter = new FileWriter(new File(folder, "OPERATION.log"));
		writers.add(opWriter);

		for (int i = 1; i <= config.getReadWorkers(); i++) {
			Writer writer = new FileWriter(new File(folder, String.format("READER-%d.log", i)));
			workers.add(new ReadWorker(DatabaseBackend.Factory.create(type), credentials, writer, start, "users"));
			writers.add(writer);
		}

		for (int i = 1; i <= config.getUpdateWorkers(); i++) {
			Writer writer = new FileWriter(new File(folder, String.format("UPDATE-%d.log", i)));
			workers.add(new UpdateWorker(DatabaseBackend.Factory.create(type), credentials, writer, start, "users"));
			writers.add(writer);
		}

		for (int i = 1; i <= config.getInsertWorkers(); i++) {
			Writer writer = new FileWriter(new File(folder, String.format("INSERT-%d.log", i)));
			workers.add(new InsertWorker(DatabaseBackend.Factory.create(type), credentials, writer, start, "users"));
			writers.add(writer);
		}

		for (int i = 1; i <= config.getDeleteWorkers(); i++) {
			Writer writer = new FileWriter(new File(folder, String.format("DELETE-%d.log", i)));
			workers.add(new DeleteWorker(DatabaseBackend.Factory.create(type), credentials, writer, start, "users"));
			writers.add(writer);
		}

		workers.stream().forEach(executor::submit);
		
		log.info("Benchmarking: {}...", operation.getName());
		workers.stream().forEach(c -> c.start());
		
		sleep(5000);

		log.info("Performing operation: {}...", operation.getName());
		long startOp = System.currentTimeMillis() - start;
		operation.perform(backend);
		
		log.info("Operation: {} completed", operation.getName());
		long endOp = System.currentTimeMillis() - start;
		
		opWriter.write("Operation\t" + startOp + "\t" + endOp + "\t" + (endOp - startOp));
		opWriter.flush();
		
		sleep(5000);
		
		workers.stream().forEach(c -> c.stop());

		executor.shutdown();
		executor.awaitTermination(60, TimeUnit.SECONDS);
		executor.shutdownNow();

		for (Writer writer : writers) {
			writer.flush();
			writer.close();
		}
		
		operation.cleanup(backend);
		backend.close();
		
		log.info("Done benchmarking: {}", operation.getName());
		return folder;
	}
	
	private void sleep(int millis) {
		try {
			Thread.sleep(millis);
		} 
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
}
