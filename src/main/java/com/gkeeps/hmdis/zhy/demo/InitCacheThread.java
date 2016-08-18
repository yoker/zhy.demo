package com.gkeeps.hmdis.zhy.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class InitCacheThread implements Runnable {
	private int MinId;
	private int MaxId;
	private CacheObject cache;
	private int Pages;
	private static int PageSize = 7000;

	public InitCacheThread(CacheObject cache, int MinId, int TotalCount) {
		this.cache = cache;
		this.MinId = MinId;
		this.MaxId = MinId + TotalCount;
		this.Pages = TotalCount % PageSize>0? (TotalCount / PageSize+1): TotalCount / PageSize;
	}

	public void run() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			String connectionUrl = String.format("jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s;",
					DbConfig.server, DbConfig.database, DbConfig.user, DbConfig.password);
			Connection con = (Connection) DriverManager.getConnection(connectionUrl);
			Statement stmt = con.createStatement();

			for (Integer i = 0; i < Pages; i++) {
				Integer pagestart = i * PageSize + MinId;
				Integer pageend = pagestart+PageSize;
				if (pageend > MaxId){
					pageend = MaxId;
				}
				String strSQL = String.format("SELECT * FROM hm_tb WHERE id>=%s AND id<%s", 
						pagestart, pageend);
				//System.out.println(strSQL);
				//continue;
				ResultSet rs = stmt.executeQuery(strSQL);
				while (rs.next()) {
					ImageHash hash = new ImageHash();
					Integer id = rs.getInt("id");
					Integer hm1 = rs.getInt("hm1");
					Integer hm2 = rs.getInt("hm2");
					Integer hm3 = rs.getInt("hm3");
					Integer hm4 = rs.getInt("hm4");
					hash.setId(id);
					hash.setHm1(hm1);
					hash.setHm2(hm2);
					hash.setHm3(hm3);
					hash.setHm4(hm4);
					cache.add(hm1, hash);
					cache.add(hm2, hash);
					cache.add(hm3, hash);
					cache.add(hm4, hash);
					cache.getTotalCount().incrementAndGet();
				}
				//System.out.print(String.format("读取完 %s - %s\r", pagestart, pageend));
			}

			stmt.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

}
