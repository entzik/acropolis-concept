package com.thekirschners.emplacements.column;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TradeTimeSeries {
	final long[] timestamps;
	final long[] products;
	final double[] pricesAndQuantities;

	final private int capacity;

	private int position;

	public TradeTimeSeries(int capacity) {
		this.capacity = capacity;

		this.timestamps = new long[capacity];
		this.products = new long[capacity];
		this.pricesAndQuantities = new double[2 * capacity];
		this.position = -1;
	}

	public void emplace(long timestamp, long product, double price, double qty) {
		this.position++;
		this.timestamps[position] = timestamp;
		this.products[position] = product;
		this.pricesAndQuantities[2 * position] = price;
		this.pricesAndQuantities[2 * position + 1] = qty;
	}

	public int[] extractTimeSlice(long minTimestamp, long maxTimestamp) {
		int[] ret = new int[]{
				findLowerTimestampBound(minTimestamp),
				findUpperTimestampBound(maxTimestamp)
		};
		return ret;
	}

	public LongStream getTimestamps() {
		return Arrays.stream(timestamps);
	}


	public TradeIterator iterator() {
		return new TradeIterator();
	}

	public Iterator<Trade> iterator(int start, int end) {
		return new TradeIterator(start, end);
	}

	public Stream<Trade> stream(int start, int end) {
		final Iterator<Trade> iterator = iterator(start, end);
		final Spliterator<Trade> spliterator
				= Spliterators.spliteratorUnknownSize(iterator, 0);
		return StreamSupport.stream(spliterator, false);
	}

	public Stream<Trade> stream() {
		final Iterator<Trade> iterator = iterator();
		final Spliterator<Trade> spliterator
				= Spliterators.spliteratorUnknownSize(iterator, 0);
		return StreamSupport.stream(spliterator, false);
	}

	private int findUpperTimestampBound(long max) {
		int maxNdx = Arrays.binarySearch(timestamps, max);
		if (maxNdx < 0)
			maxNdx = (-maxNdx) - 1;
		while (maxNdx < (timestamps.length - 1) && timestamps[maxNdx] == timestamps[maxNdx + 1])
			maxNdx++;
		return maxNdx;
	}

	private int findLowerTimestampBound(long min) {
		int minNdx = Arrays.binarySearch(timestamps, min);
		if (minNdx < 0)
			minNdx = (-minNdx) - 1;
		while (minNdx > 0 && timestamps[minNdx] == timestamps[minNdx - 1])
			minNdx--;
		return minNdx;
	}

	public int getCapacity() {
		return capacity;
	}

	public int getSize() {
		return position + 1;
	}


	public class TradeIterator implements Trade, Iterator<Trade> {
		private int crtNdx;
		private final int end;

		protected TradeIterator() {
			this.crtNdx = 1;
			this.end = TradeTimeSeries.this.position;
		}

		public TradeIterator(int start, int end) {
			this.crtNdx = start - 1;
			this.end = end;
		}

		@Override
		public long getTimestamp() {
			return TradeTimeSeries.this.timestamps[crtNdx];
		}

		@Override
		public void setTimestamp(long timeStamp) {
			TradeTimeSeries.this.timestamps[crtNdx] = timeStamp;
		}

		@Override
		public void setProductId(long productId) {
			TradeTimeSeries.this.products[crtNdx] = productId;
		}

		@Override
		public long getProductId() {
			return TradeTimeSeries.this.products[crtNdx];
		}

		@Override
		public void setPrice(double price) {
			TradeTimeSeries.this.pricesAndQuantities[crtNdx * 2] = price;
		}

		@Override
		public double getPrice() {
			return TradeTimeSeries.this.pricesAndQuantities[crtNdx * 2];
		}

		@Override
		public void setQuantity(double quantity) {
			TradeTimeSeries.this.pricesAndQuantities[crtNdx * 2 + 1] = quantity;
		}

		@Override
		public double getQuantity() {
			return TradeTimeSeries.this.pricesAndQuantities[crtNdx * 2 + 1];
		}

		@Override
		public boolean hasNext() {
			return this.crtNdx < end;
		}

		@Override
		public TradeIterator next() {
			crtNdx++;
			return this;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void forEachRemaining(Consumer<? super Trade> action) {
			while (hasNext())
				action.accept(next());
		}
	}

	public double calculateAverageNotional() {
		return stream().
				mapToDouble(trade -> trade.getPrice() * trade.getQuantity())
				.average()
				.orElse(Double.NaN);
	}

}
