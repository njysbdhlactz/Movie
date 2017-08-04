package oracle.demo.oow.bd.dao.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import oracle.demo.oow.bd.util.hbase.ConstantsHBase;

public class BaseDao
{

	private static Connection conn;
	private static Statement stmt = null;

	public static Connection getOraConnect()
	{
		return getOraConnect(ConstantsHBase.MYSQL_DB_USER,
				ConstantsHBase.MYSQL_DB_PASSWORD);
	}

	public static Connection getOraConnect(String user, String password)
	{

		try
		{
			Class.forName("com.mysql.jdbc.Driver");
			if (conn == null)
			{
				conn = DriverManager.getConnection(ConstantsHBase.JDBC_URL,
						user, password);
				conn.setAutoCommit(true);
				stmt = conn.createStatement();
				System.out.println("Connected to database");
			}

		} catch (SQLException se)
		{
			// se.printStackTrace();
		} catch (Exception e)
		{
			// e.printStackTrace();
		}
		return conn;
	}

	public static ResultSet executeQuery(String s)
	{
		
		ResultSet rs = null;
		try
		{

			rs = stmt.executeQuery(s);
		} catch (Exception ex)
		{
			System.out.println("查询执行失败！");
		}
		return rs;
	}
}
