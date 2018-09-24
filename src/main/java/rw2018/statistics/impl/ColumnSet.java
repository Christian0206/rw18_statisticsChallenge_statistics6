package rw2018.statistics.impl;

import java.util.LinkedList;
import java.util.List;

public class ColumnSet {

	private LinkedList<reducedRow> rows;

	public ColumnSet() {
		this.rows = new LinkedList<>();
	}

	public ColumnSet(List<reducedRow> rows) {
		this.rows = new LinkedList<>(rows);
	}

	public LinkedList<reducedRow> getRows() {
		return rows;
	}

	public void updateRows(reducedRow row) {
		this.rows.add(row);
	}

}
