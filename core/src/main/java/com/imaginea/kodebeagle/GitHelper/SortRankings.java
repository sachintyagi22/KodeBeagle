package com.imaginea.kodebeagle.GitHelper;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class SortRankings {

	/**
	 * This method sorts the file rankings using comparator
	 *
	 * @param fileRankings
	 */
	@SuppressWarnings("unchecked")
	public static Map<String,Integer> sortRankingsMap(Map<String, Integer> fileRankings, int n) {
		int count = 0;
		Map<String,Integer> topNFiles = new HashMap<String, Integer>();
		Object[] a = fileRankings.entrySet().toArray();
		Arrays.sort(a, new Comparator<Object>() {
			public int compare(Object o1, Object o2) {
				return ((Map.Entry<String, Integer>) o2).getValue()
						.compareTo(((Map.Entry<String, Integer>) o1).getValue());
			}
		});
		for (Object e : a) {
			if (count < n) {
				topNFiles.put(((Map.Entry<String, Integer>) e).getKey(), ((Map.Entry<String, Integer>) e).getValue());
			}
		}
		return null;
	}
}