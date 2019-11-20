package jdbc.mysql.jdbcApp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Demo 
{
	public static void main(String[] args) 
	{
		Connection con=null;
		Statement stmt=null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("loaded & registered");
			con=DriverManager.getConnection("jdbc:mysql://localhost:3306?user=root&password=root");
			System.out.println("connected to db");
			stmt=con.createStatement();
			String sql1="SELECT COUNT(DISTINCT supplier_gstin) AS count1 FROM test.test_data";
			String sql2="SELECT COUNT(DISTINCT purchaser_gstin) AS count2 FROM test.test_data";
			String sql3="SELECT supplier_gstin,purchaser_gstin, COUNT(1) as count3 FROM test.test_data group by supplier_gstin,purchaser_gstin";
			String sql4="SELECT supplier_gstin,purchaser_gstin,SUM(val) as sum from test.test_data group by supplier_gstin,purchaser_gstin";
			int count1=0;
			int count2=0;
			int count3=0;
			String sup="";
			String pur="";
			System.out.println("statement created");
			ResultSet rs1 = stmt.executeQuery(sql1);
		    while(rs1.next())
		    {
		    	count1 = rs1.getInt("count1");
		    }
		    System.out.println("Count of supplier_gstin"+count1);
		    
		    ResultSet rs2 = stmt.executeQuery(sql2);
		    while(rs2.next())
		    {
		    	count2 = rs2.getInt("count2");
		    }
		    System.out.println("Count of Purchaser_gstin"+count2);
		    System.out.println("--------------------------------------");
		    System.out.println("supplier_gstin  | purchaser_gstin | no. of rows");
		    System.out.println("--------------------------------------");
		    ResultSet rs3=stmt.executeQuery(sql3);
		    while(rs3.next())
		    {
		    	count3 = rs3.getInt("count3");
		    	sup=rs3.getString("supplier_gstin");
		    	pur=rs3.getString("purchaser_gstin");
		    	System.out.println(sup+" | "+pur+" - "+count3);
		    }
		    System.out.println("--------------------------------------");
		    System.out.println("supplier_gstin  | purchaser_gstin | sum of values");
		    System.out.println("--------------------------------------");
		   
		    ResultSet rs4=stmt.executeQuery(sql4);
		    
		    while(rs4.next())
		    {
		    	System.out.println(rs4.getString("supplier_gstin")+" | "+rs4.getString("supplier_gstin")+" | "+rs4.getInt("sum"));;
		    	
		    }
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		finally 
		{
			if(stmt!=null)
			{
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(con!=null)
			{
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
