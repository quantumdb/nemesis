package io.quantumdb.nemesis;

import javax.imageio.ImageIO;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;





public class Grapher {

	private static final int PADDING = 20;

	public static void main(String[] args) throws IOException {
		
		new Grapher().graphResponseTimes(new File("/Users/michael/logs/POSTGRES/add-nullable-foreign-key/"));
		
	}
	
	public void graphResponseTimes(File folder) throws IOException {
		int duration = getDuration(folder);

		if (duration > 20000) {
			drawSplit(duration, folder);
		}
		else {
			drawAll(duration, folder);
		}
	}

	private void drawAll(int duration, File folder) throws IOException {
		BufferedImage image = new BufferedImage(
				(int) (duration + 2 * PADDING),
				100 + 2 * PADDING,
				BufferedImage.TYPE_USHORT_555_RGB);

		Graphics graphics = image.getGraphics();

		graphics.setColor(Color.WHITE);
		graphics.fillRect(0, 0, image.getWidth(), image.getHeight());

		File[] logFiles = folder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".log");
			}
		});

		for (File file : logFiles) {
			parse(file, line -> {
				String queryType = getWorkerType(line);
				long queryStart = getQueryStart(line);
				long queryEnd = getQueryEnd(line);
				long queryDuration = queryEnd - queryStart;

				double xResolution = (double) duration / (image.getWidth() - 2 * PADDING);

				int x = (int) (queryStart / xResolution);
				int y = (int) queryDuration;

				if (queryType.equals("Operation")) {
					graphics.setColor(new Color(0f, 0f, 0f, 0.1f));
					graphics.fillRect(x + PADDING, PADDING, (int) (queryDuration / xResolution), image.getHeight() - 2 * PADDING);
				}

				switch (queryType) {
					case "InsertWorker":
						graphics.setColor(Color.GREEN);
						break;
					case "DeleteWorker":
						graphics.setColor(Color.RED);
						break;
					case "UpdateWorker":
						graphics.setColor(Color.BLUE);
						break;
					case "ReadWorker":
						graphics.setColor(Color.ORANGE);
						break;
					case "Operation":
						graphics.setColor(Color.BLACK);
				}

				graphics.drawLine(x + PADDING, Math.max(0, image.getHeight() - PADDING - y), x + PADDING, image.getHeight() - PADDING);
			});
		}

		ImageIO.write(image, "png", new File(folder, "response-times.png"));
	}

	private void drawSplit(int duration, File folder) throws IOException {
		BufferedImage image = new BufferedImage(
				(int) (10_000 + 3 * PADDING),
				100 + 2 * PADDING,
				BufferedImage.TYPE_USHORT_555_RGB);

		Graphics graphics = image.getGraphics();

		graphics.setColor(Color.WHITE);
		graphics.fillRect(0, 0, image.getWidth(), image.getHeight());

		File[] logFiles = folder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".log");
			}
		});

		for (File file : logFiles) {
			parse(file, line -> {
				String queryType = getWorkerType(line);
				long queryStart = getQueryStart(line);
				long queryEnd = getQueryEnd(line);
				int queryDuration = (int) (queryEnd - queryStart);

				double xResolution = (double) duration / (image.getWidth() - 2 * PADDING);

				int x = (int) (queryStart / xResolution);
				int y = queryDuration;

				if (x > 10_000) {
					return;
				}

				if (queryType.equals("Operation")) {
					graphics.setColor(new Color(0f, 0f, 0f, 0.1f));
					graphics.fillRect(x + PADDING, PADDING, (int) (queryDuration / xResolution), image.getHeight() - 2 * PADDING);
				}

				switch (queryType) {
					case "InsertWorker":
						graphics.setColor(Color.GREEN);
						break;
					case "DeleteWorker":
						graphics.setColor(Color.RED);
						break;
					case "UpdateWorker":
						graphics.setColor(Color.BLUE);
						break;
					case "ReadWorker":
						graphics.setColor(Color.ORANGE);
						break;
					case "Operation":
						graphics.setColor(Color.BLACK);
				}

				graphics.drawLine(x + PADDING, Math.max(0, image.getHeight() - PADDING - y), x + PADDING, image.getHeight() - PADDING);
			});
		}

		ImageIO.write(image, "png", new File(folder, "response-times.png"));
	}
	
	private int getDuration(File folder) throws IOException {
		File[] logFiles = folder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".log");
			}
		});

		long start = Long.MAX_VALUE;
		long end = Long.MIN_VALUE;
		
		for (File file : logFiles) {
			start = Math.min(start, getQueryStart(firstLine(file)));
			end = Math.max(end, getQueryEnd(lastLine(file)));
		}
		
		return (int) (end - start);
	}
	
	private String lastLine(File file) throws IOException {
		try (RandomAccessFile accessor = new RandomAccessFile(file, "r")) {
			long length = accessor.length();
			
			accessor.seek(Math.max(0, length - 100));
			
			String lastLine = null;
			String currentLine = null;
			while ((currentLine = accessor.readLine()) != null) {
				lastLine = currentLine;
			}
			return lastLine;
		}
	}
	
	private String firstLine(File file) throws IOException {
		try (RandomAccessFile accessor = new RandomAccessFile(file, "r")) {
			return accessor.readLine();
		}
	}
	
	private void parse(File file, Parser parser) throws IOException {
		try (RandomAccessFile accessor = new RandomAccessFile(file, "r")) {
			boolean read = true;
			do {
				String line = accessor.readLine();
				if (line == null) {
					read = false;
				}
				else {
					parser.parse(line);
				}
			}
			while (read);
		}
	}
	
	private String getWorkerType(String line) {
		return line.substring(0, line.indexOf('\t'));
	}
	
	private long getQueryStart(String line) {
		int first = line.indexOf('\t');
		return Long.parseLong(line.substring(first + 1, line.indexOf('\t', first + 1)));
	}
	
	private long getQueryEnd(String line) {
		int first = line.indexOf('\t');
		int second = line.indexOf('\t', first + 1);
		return Long.parseLong(line.substring(second + 1, line.indexOf('\t', second + 1)));
	}
	
	@FunctionalInterface
	private interface Parser {
		void parse(String line);
	}
	
}
