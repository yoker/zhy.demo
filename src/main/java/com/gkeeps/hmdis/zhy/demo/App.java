package com.gkeeps.hmdis.zhy.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
	public static void main(String[] args) throws Exception {

		CacheObject data = new CacheObject();

		int threadCount = 30;
		final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);

		long starticks = System.currentTimeMillis();
		int tasknum = 300;
		int recordscount = 5000001;
		int pagesize = recordscount % tasknum > 0 ? recordscount / tasknum + 1 : recordscount / tasknum;
		for (int i = 0; i < tasknum; i++) {
			int minid = pagesize * i + 1;
			sendThreadPool.execute(new InitCacheThread(data, minid, pagesize));
		}
		// when all task end. then close threadpool.
		sendThreadPool.shutdown();

		while(!sendThreadPool.isTerminated()){
			Thread.sleep(300);
			String output = String.format("%s records Completed.", data.GetRecords());
			System.out.print(output);
			for (int j = 0; j <= String.valueOf(output).length(); j++) {
	            System.out.print("\b");
	        }
		}
		long finishticks = System.currentTimeMillis();
		System.out.println(String.format("\nload data Completed. %s ms", finishticks-starticks));
		
		Scanner in = new Scanner(System.in);
		while(true){
			System.out.println("Please Input dhash to test(eg: 11,54960,5634,35976), Enter 'q' to Exit:");
			String input = in.next();
			if(input.equals("q")){
				break;
			}
			String[] s = input.split(",");
			if(s.length < 4){
				System.out.println("error input format.");
				continue;
			}
	        int hm1 = Integer.parseInt(s[0]);
	        int hm2 = Integer.parseInt(s[1]);
	        int hm3 = Integer.parseInt(s[2]);
	        int hm4 = Integer.parseInt(s[3]);

	        Long t1 = System.currentTimeMillis();

			List<Integer> HashList = new ArrayList<Integer>();
			HashList.add(hm1);
			HashList.add(hm2);
			HashList.add(hm3);
			HashList.add(hm4);
			for (int i = 1; i < 65535; i = i << 1) {
				//System.out.println(i);
				HashList.add(hm1 ^ i);
				HashList.add(hm2 ^ i);
				HashList.add(hm3 ^ i);
				HashList.add(hm4 ^ i);
			}
			
			List<Integer> existHash = new ArrayList<Integer>();
			
			for(Integer i : HashList){
				List<ImageHash> hashs = data.getItems(i);
		        for(ImageHash hash : hashs){
		        	int bit1 = Integer.bitCount(hash.getHm1()^hm1);
		        	int bit2 = Integer.bitCount(hash.getHm2()^hm2);
		        	int bit3 = Integer.bitCount(hash.getHm3()^hm3);
		        	int bit4 = Integer.bitCount(hash.getHm4()^hm4);
					if (bit1 + bit2 + bit3 + bit4 <= 4) {
						if(existHash.contains(hash.getId())){
							continue;
						}
						existHash.add(hash.getId());
						System.out.println(String.format("id=%s, hm1=%s, hm2=%s,hm3=%s, hm4=%s", hash.getId(), hash.getHm1(),
								hash.getHm2(), hash.getHm3(), hash.getHm4()));
					}
		        }
			}
			Long times = System.currentTimeMillis() - t1;
			String output = String.format("found %s hash, times: %s ms\n", existHash.size(), times);
			System.out.println(output);
		}
        in.close();
	}

	/**
	 * 从数据库中查询结果
	 * @throws Exception
	 */
	void SearchFromDB(int hm1, int hm2, int hm3, int hm4) throws Exception {
		long beginticks = System.currentTimeMillis();

		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionUrl = String.format("jdbc:sqlserver://%s;databaseName=%s;user=%s;password=%s;",
				DbConfig.server, DbConfig.database, DbConfig.user, DbConfig.password);
		Connection con = (Connection) DriverManager.getConnection(connectionUrl);
		Statement stmt = con.createStatement();
		String strSQL = String.format("exec getrows %s,%s,%s,%s", hm1, hm2, hm3, hm4);
		ResultSet rs = stmt.executeQuery(strSQL);
		while (rs.next()) {
			 System.out.println(String.format("id=%s, hm1=%s, hm2=%s,hm3=%s, hm4=%s",
			 rs.getInt("id"),
			 rs.getInt("hm1"),
			 rs.getInt("hm2"),
			 rs.getInt("hm3"),
			 rs.getInt("hm4")));
		}
		stmt.close();
		con.close();

		long endticks = System.currentTimeMillis();
		System.out.println(String.format("Time %s ms.", endticks - beginticks));
	}
}
