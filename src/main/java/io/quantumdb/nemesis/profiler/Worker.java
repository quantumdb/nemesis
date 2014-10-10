package io.quantumdb.nemesis.profiler;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public abstract class Worker implements Runnable {
	
	private final AtomicBoolean running = new AtomicBoolean();
	
	private final Database backend;
	private final DatabaseCredentials credentials;
	private final Writer writer;
	private final long startingTimestamp;

	@Override
	public final void run() {
		String type = getClass().getSimpleName();
		
		try {
			backend.connect(credentials);
		} 
		catch (SQLException e) {
			log.error(e.getMessage(), e);
			return;
		}

		while (!running.get()) {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				return;
			}
		}
		
		log.debug("{} is running...", type);
		
		while (running.get()) {
			try {
				long start = System.currentTimeMillis();
				doAction();
				long end = System.currentTimeMillis();
				writer.write(type + "\t" + (start - startingTimestamp) + "\t" + (end - startingTimestamp) + "\t" + (end - start) + "\n");
			}
			catch (IOException | SQLException e) {
				log.warn(e.getMessage(), e);
			}
		}
		
		try {
			backend.close();
		}
		catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		
		log.debug("{} has finished", type);
	}
	
	abstract void doAction() throws SQLException;

	public void stop() {
		running.set(false);
	}
	
	public void start() {
		running.set(true);
	}

}
