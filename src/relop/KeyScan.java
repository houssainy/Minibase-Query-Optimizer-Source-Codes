package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	private final HeapFile file;
	private final HashScan hashScan;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */

	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {

		this.setSchema(schema);
		this.file = file;
		// Initiates an equality scan of the index file with specified key.
		hashScan = index.openScan(key);

	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	@Override
	public void explain(int depth) {
		// TODO: KeyScan explain
		// use indent method in iterator
		this.indent(depth);

	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	@Override
	public void restart() {
		// TODO: KeyScan restart
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	@Override
	public boolean isOpen() {

		if (hashScan != null)
			return true;
		return false;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	@Override
	public void close() {
		if (hashScan != null)
			hashScan.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	@Override
	public boolean hasNext() {

		if (hashScan.hasNext())
			return true;
		return false;

	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	@Override
	public Tuple getNext() {

		if (hashScan != null && hashScan.hasNext()) {
			byte[] data = file.selectRecord(hashScan.getNext());
			Tuple tuple = new Tuple(this.getSchema(), data);
			return tuple;
		}
		return null;

	} // public class KeyScan extends Iterator
}
