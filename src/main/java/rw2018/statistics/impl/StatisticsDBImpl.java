package rw2018.statistics.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import rw2018.statistics.StatisticsDB;
import rw2018.statistics.TriplePosition;

/**
 * This is the class that will be executed during the evaluation!!
 * 
 * TODO implement this class
 */
public class StatisticsDBImpl implements StatisticsDB {

	File statisticsFile;
	RandomAccessFile statistics;
	ColumnSet s, p, o;
	int numberOfChunks;
	Map<Long, Integer> location = new HashMap<>();

	@Override
	public void setUp(File statisticsDir, int numberOfChunks) {
		s = new ColumnSet();
		p = new ColumnSet();
		o = new ColumnSet();

		this.numberOfChunks = numberOfChunks;
		if (!statisticsDir.exists()) {
			statisticsDir.mkdirs();
		}
		this.statisticsFile = new File(statisticsDir.getAbsolutePath() + File.separator + "statistics");
		try {
			statistics = new RandomAccessFile(statisticsFile, "rw");
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public int getNumberOfChunks() {
		return this.numberOfChunks;

	}

	@Override
	public void incrementFrequency(long resourceId, int chunkNumber, TriplePosition triplePosition) {
		// TODO Auto-generated method stub
		int x = 1;
		if (triplePosition.equals(TriplePosition.SUBJECT)) {
			if (location.containsKey(resourceId)) {
				x = location.get(resourceId);
				reducedRow r = s.getRows().get(x);
				r.increaseFrequency();
			} else {
				s.updateRows(new reducedRow(chunkNumber, 1));
				location.put(resourceId, s.getRows().size() - 1);
			}
		} else if (triplePosition.equals(TriplePosition.OBJECT)) {
			if (location.containsKey(resourceId)) {
				x = location.get(resourceId);
				reducedRow r = o.getRows().get(x);
				r.increaseFrequency();
			} else {
				o.updateRows(new reducedRow(chunkNumber, 1));
				location.put(resourceId, o.getRows().size() - 1);
			}
		} else if (triplePosition.equals(TriplePosition.PROPERTY)) {
			if (location.containsKey(resourceId)) {
				x = location.get(resourceId);
				reducedRow r = p.getRows().get(x);
				r.increaseFrequency();
			} else {
				p.updateRows(new reducedRow(chunkNumber, 1));
				location.put(resourceId, p.getRows().size() - 1);
			}
		}
	}

	@Override
	public long getFrequency(long resourceId, int chunkNumber, TriplePosition triplePosition) {
		int x = -1;
		if (triplePosition.equals(TriplePosition.PROPERTY)) {
			if (location.containsKey(resourceId)) {
				x = location.get(resourceId);
				reducedRow r = p.getRows().get(x);
				return r.getFrequency();
			}
		} else if (triplePosition.equals(TriplePosition.OBJECT)) {
			if (location.containsKey(resourceId)) {
				x = location.get(resourceId);
				reducedRow r = o.getRows().get(x);
				return r.getFrequency();
			}
		} else if (triplePosition.equals(TriplePosition.SUBJECT)) {
			if (location.containsKey(resourceId)) {
				x = location.get(resourceId);
				reducedRow r = s.getRows().get(x);
				return r.getFrequency();
			}
		}
		return 0;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
