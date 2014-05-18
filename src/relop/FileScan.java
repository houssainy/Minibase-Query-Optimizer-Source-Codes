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


	/**
	 * Constructs a file scan, given the schema and heap file.
	 */
	public FileScan(Schema schema, HeapFile heapFile) {
		this.heapFile = heapFile;
		
		setSchema(schema);
		
		this.heapScan = heapFile.openScan();
		this.lastRec = new RID();
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
		if(isOpen())
			heapScan.close();
		heapScan = heapFile.openScan();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return heapScan != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if (heapScan != null)
			heapScan.close();

		heapScan = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return heapScan.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		byte[] rec = heapScan.getNext(lastRec);
		return new Tuple(getSchema(), rec);
	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID() {
		return lastRec;
	}

} // public class FileScan extends Iterator