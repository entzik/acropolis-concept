package com.thekirschners.emplacements.column;

public interface Trade {
	void setTimestamp(long time);
	long getTimestamp();

	void setProductId(long prodictId);
	long getProductId();

	void setPrice(double price);
	double getPrice();

	void setQuantity(double quantity);
	double getQuantity();
}
