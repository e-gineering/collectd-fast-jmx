package org.collectd;

import org.collectd.api.Collectd;

import java.util.ArrayList;
import java.util.List;

/**
 * A small, ring buffer....
 */
public class Ring<T> {
	private Object[] ring;
	private int index = 0;

	public Ring(final int size) {
		if (size > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Rings cannot be bigger than Integer.MAX_VALUE");
		}
		this.ring = new Object[size];
		this.index = 0;
	}

	public void grow(final int newCapacity) {
		if (newCapacity > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Rings cannot be bigger than Integer.MAX_VALUE");
		}
		if (newCapacity < ring.length) {
			throw new IllegalArgumentException("Rings to not support shrinking. newCapacity must be larger than the existing size.");
		}

		Object[] existing = this.ring;
		this.ring = new Object[newCapacity];
		System.arraycopy(existing, 0, this.ring, 0, existing.length);
		if (index == 0) {
			index = existing.length;
		}
	}

	public void push(T object) {
		if (object == null) {
			throw new IllegalArgumentException("Ring buffer does not support 'null' values.");
		}
		ring[index] = object;
		this.index = (index + 1) % ring.length;
	}

	public T peek() {
		int pos = index;
		if (pos == 0) {
			pos = ring.length;
		}
		return (T)(ring[pos - 1]);
	}

	public List<T> peek(int count) {
		int pos = index - count;
		if (pos < 0) {
			pos = ring.length + pos;
		}
		ArrayList<T> list = new ArrayList<T>(count);
		for (int i = 0; i < count; i++) {
			list.add(i, (T)(ring[pos]));
			pos = (pos + 1) % ring.length;
		}
		return list;
	}

	public void shift(T object) {
		if (object == null) {
			throw new IllegalArgumentException("Ring buffer does not support 'null' values.");
		}
		ring[index = (index - 1) % ring.length] = object;
	}

	public T get(int i) {
		if (i < 0 || i > ring.length - 1) {
			throw new IndexOutOfBoundsException("Index " + i);
		}

		return (T) ring[i];
	}

	public int getIndex() {
		return index;
	}

	public int capacity() {
		return ring.length;
	}

	public int size() {
		for (int i = 0; i < ring.length; i++) {
			if (ring[i] == null) {
				return i;
			}
		}
		return ring.length;
	}
}
