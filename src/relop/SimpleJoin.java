package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

	private Iterator leftIterator, rightIterator;
	private Predicate[] preds;

	private Tuple currentTuple = null;

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
		this.leftIterator = left;
		this.rightIterator = right;
		this.preds = preds;

		// Build new Schema
		//TODO
		this.setSchema(Schema.join(left.getSchema(), right.getSchema()));

		prepareNextTuple();
	}

	// Join nested loop
	private void prepareNextTuple() {
		Tuple leftTuple = null;
		Tuple righTuple = null;
		
		currentTuple = null;
		pl:while (leftIterator.hasNext()) {// First table
			
			leftTuple = leftIterator.getNext();
			while (rightIterator.hasNext()) { // Second table
				righTuple = rightIterator.getNext();
				
				Tuple joinTuple = Tuple.join(leftTuple, righTuple, getSchema());
				
				boolean passed = true; 
				for (int i = 0; i < preds.length; i++) {
					if( !preds[i].evaluate(joinTuple)){
						passed = false;
						break;
					}
				}
				
				if( passed ){
					currentTuple =  joinTuple;
					break pl;
				}
			}
		}
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
		leftIterator.restart();
		rightIterator.restart();
		
		prepareNextTuple();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return (leftIterator.isOpen() && rightIterator.isOpen());
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		leftIterator.close();
		rightIterator.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return currentTuple != null;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (currentTuple != null) { // if no selection has been found, - for
									// initialization of the class -
			Tuple tuple = currentTuple;
			prepareNextTuple();
			return tuple;
		}

		throw new IllegalStateException("There is no next!");
	}

} // public class SimpleJoin extends Iterator
