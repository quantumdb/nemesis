package io.quantumdb.nemesis.profiler;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.operations.NamedOperation;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Session {

	private final Database.Type type;
	private final ProfilerConfig config;
	private final DatabaseCredentials credentials;
	private final int startupTimeout;
	private final int teardownTimeout;

	public Session(Database.Type type, ProfilerConfig config, DatabaseCredentials credentials, int startupTimeout,
			int teardownTimeout) {

		this.type = type;
		this.config = config;
		this.credentials = credentials;
		this.startupTimeout = startupTimeout;
		this.teardownTimeout = teardownTimeout;
	}

	public File start(NamedOperation operation) throws Exception {
		File folder = null;
		ScheduledThreadPoolExecutor executor = null;
		Database backend = type.createBackend();

		if (!operation.isSupportedBy(backend)) {
			log.warn("Database: {} does not support operation: {}", backend, operation.getName());
			return null;
		}

		List<Worker> workers = Lists.newArrayList();
		List<Writer> writers = Lists.newArrayList();

		try {
			executor = new ScheduledThreadPoolExecutor(config.getTotalWorkers() + 1);

			backend.connect(credentials);
			operation.prepare(backend);

			sleep(100);

			folder = new File("logs/" + type + "/" + operation.getName() + "/");
			folder.mkdirs();

			long start = System.currentTimeMillis();

			Writer opWriter = new FileWriter(new File(folder, "OPERATION.log"));
			writers.add(opWriter);

			for (int i = 1; i <= config.getReadWorkers(); i++) {
				Writer writer = new FileWriter(new File(folder, String.format("READER-%d.log", i)));
				workers.add(new SelectWorker(type.createBackend(), credentials, writer, start, "users"));
				writers.add(writer);
			}

			for (int i = 1; i <= config.getUpdateWorkers(); i++) {
				Writer writer = new FileWriter(new File(folder, String.format("UPDATE-%d.log", i)));
				workers.add(new UpdateWorker(type.createBackend(), credentials, writer, start, "users"));
				writers.add(writer);
			}

			for (int i = 1; i <= config.getInsertWorkers(); i++) {
				Writer writer = new FileWriter(new File(folder, String.format("INSERT-%d.log", i)));
				workers.add(new InsertWorker(type.createBackend(), credentials, writer, start, "users"));
				writers.add(writer);
			}

			for (int i = 1; i <= config.getDeleteWorkers(); i++) {
				Writer writer = new FileWriter(new File(folder, String.format("DELETE-%d.log", i)));
				workers.add(new DeleteWorker(type.createBackend(), credentials, writer, start, "users"));
				writers.add(writer);
			}

			workers.stream().forEach(executor::submit);

			log.info("Benchmarking: {}...", operation.getName());
			workers.stream().forEach(c -> c.start());

			sleep(startupTimeout);

			log.info("\tPerforming operation: {}...", operation.getName());
			long startOp = System.currentTimeMillis() - start;

			Future<?> future = executor.submit(() -> {
				try {
					operation.perform(backend);
				}
				catch (Exception e) {
					log.error(e.getMessage(), e);
				}
			});

			try {
				future.get();
			}
			catch (ExecutionException e) {
				log.error(e.getMessage(), e);
			}

			log.info("\tOperation: {} completed", operation.getName());
			long endOp = System.currentTimeMillis() - start;

			opWriter.write("Operation\t" + startOp + "\t" + endOp + "\t" + (endOp - startOp));
			opWriter.flush();

			sleep(teardownTimeout);
		}
		finally {
			workers.stream().forEach(c -> c.stop());

			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.MINUTES);
			executor.shutdownNow();

			for (Writer writer : writers) {
				writer.flush();
				writer.close();
			}

			try {
				operation.cleanup(backend);
			}
			finally {
				backend.close();
			}
		}

		log.info("\tDone benchmarking: {}", operation.getName());
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
