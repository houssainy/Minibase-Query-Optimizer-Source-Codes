package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	private Iterator iterator;
	private Integer[] fields;
	
	/**
	 * Constructs a projection, given the underlying iterator and field numbers.
	 */
	public Projection(Iterator iter, Integer... fields) {
		this.iterator = iter;
		this.fields = fields;
		
		// Create the new Schema
		Schema schema = new Schema(fields.length);
		for (int i = 0; i < fields.length; i++)
			schema.initField(i, iter.getSchema().fieldType(fields[i]), iter.getSchema()
					.fieldLength(fields[i]), iter.getSchema().fieldName(fields[i]));
		

		this.setSchema(schema);
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
		iterator.restart();
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
		return iterator.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if(iterator.hasNext()){
			Object[] data = new Object[fields.length];
			Object[] tuple = iterator.getNext().getAllFields();
			
			for (int i = 0; i < data.length; i++) 
				data[i] = tuple[fields[i]];
			
			return new Tuple(getSchema() ,  data);
		}
		
		throw new IllegalStateException("There is no next!");
	}

} // public class Projection extends Iterator
