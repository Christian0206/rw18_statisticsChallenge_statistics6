package rw2018.statistics.impl;

public class reducedRow {

	private final int name;
	private int frequency;

	public reducedRow(int chunckName, int frequency) {
		this.name = chunckName;
		this.frequency = frequency;
	}

	public int getName() {
		return name;
	}

	public int getFrequency() {
		return frequency;
	}

	public void increaseFrequency() {
		this.frequency = this.frequency + 1;
	}

}
