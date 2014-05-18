package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by AND operators.
 */
public class Selection extends Iterator {

	private Iterator iterator;
	private Predicate[] preds;

	private Tuple currentTuple = null;

	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(Iterator iter, Predicate... preds) {
		this.iterator = iter;
		this.preds = preds;

		setSchema(iter.getSchema());

		prepareNext();
	}

	private void prepareNext() {

		currentTuple = null;
		boolean isSelection;

		while (iterator.hasNext()) {
			currentTuple = iterator.getNext();
			isSelection = true;

			for (int i = 0; i < preds.length; i++) { // Check if the current
														// tuple satisfy all
														// conditions
				if (!preds[i].evaluate(currentTuple)) {
					isSelection = false;
					break;
				}
			}

			if (isSelection) // found a tuple
				break;

			currentTuple = null;
		}

	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		iterator.explain(depth);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		iterator.restart();
		prepareNext();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return iterator.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iterator.close();
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
			prepareNext();
			return tuple;
		}

		throw new IllegalStateException("There is no next!");

	}

} // public class Selection extends Iterator
