package com.thekirschners.emplacements.column;

import java.util.ArrayList;

/**
 * Created by emilkirschner on 29/05/17.
 */
public class ClassicMemoryProbe {
	private static ArrayList<EnvObservationImpl> list;

	public static void main(String[] args) {

		list = new ArrayList<>();
		Runtime.getRuntime().gc();
		final long initialMemory = Runtime.getRuntime().freeMemory();
		for (long i = 0; i < 100000000; i ++) {
			final double temperature = Math.random() * 1000d;
			final double humidity = Math.random() * 1000d;
			final double windSpeed = Math.random() * 1000d;
			list.add(new EnvObservationImpl(System.currentTimeMillis(), temperature, humidity, windSpeed));
		}
		Runtime.getRuntime().gc();
		final long postMemory = Runtime.getRuntime().freeMemory();
		final long deltaMemory = initialMemory - postMemory;
		System.out.println("deltaMemory = " + deltaMemory);
		System.out.println(list.size());
	}

}
