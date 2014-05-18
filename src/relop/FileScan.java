package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

	private HeapFile heapFile;
	private HeapScan heapScan;
	private RID lastRec;
	private boolean isOpen;

	private Schema schema;

	/**
	 * Constructs a file scan, given the schema and heap file.
	 */
	public FileScan(Schema schema, HeapFile heapFile) {
		heapScan = heapFile.openScan();
		isOpen = true;
		this.heapFile = heapFile;
		this.schema = schema;
		lastRec = new RID();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if (heapScan != null) {
			heapScan.close();
		}
		heapScan = heapFile.openScan();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return isOpen;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if (heapScan != null) {
			heapScan.close();
		}
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (heapScan != null) {
			return heapScan.hasNext();
		}
		return false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		Tuple tuple = null;
		if (heapScan != null) {
			byte[] rec = heapScan.getNext(lastRec);
			tuple = new Tuple(schema, rec);
		}
		return tuple;
	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID() {
		return lastRec;
	}

} // public class FileScan extends Iterator