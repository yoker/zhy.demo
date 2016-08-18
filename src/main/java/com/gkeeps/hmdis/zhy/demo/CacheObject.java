package com.gkeeps.hmdis.zhy.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CacheObject {
	private final AtomicLong TotalCount = new AtomicLong(0L);

	private HashMap<Integer, List<ImageHash>> data;
	public CacheObject(){
		data = new HashMap<Integer, List<ImageHash>>();
	}
	
	public synchronized void add(int key, ImageHash hash){
		List<ImageHash> items = data.get(key);
		if(items == null){
			items = new ArrayList<ImageHash>();
		}
		items.add(hash);
		data.put(key, items);
	}
	
	public List<ImageHash> getItems(Integer key){
		if (data.containsKey(key)) {
			return data.get(key);
		} else {
			return new ArrayList<ImageHash>();
		}
	}
	
	public HashMap<Integer, List<ImageHash>> getData(){
		return data;
	}
	
	public AtomicLong getTotalCount() {
		return TotalCount;
	}

	public Long GetRecords(){
		return TotalCount.get();
	}
}
