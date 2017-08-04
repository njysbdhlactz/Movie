package oracle.demo.oow.bd.util.hbase;

import java.io.IOException;

import oracle.demo.oow.bd.config.StoreConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDB
{
	private Connection conn;
	

	private static class HbaseDBInstance
	{
		private static final HBaseDB instance =new HBaseDB();
	}
	public static HBaseDB getInstance()
	{
		return HbaseDBInstance.instance;
	}

	private HBaseDB()
	{
		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", ConstantsHBase.HBSTORE_NAME);
		conf.set("hbase.rootdir", ConstantsHBase.HBSTORE_URL);
		try
		{
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e)
		{
			System.out.println("ERROR: Please make sure Hadoop Hbase Database is up and running at '" +
					ConstantsHBase.HBSTORE_URL + "' with host name as: '" + ConstantsHBase.HBSTORE_URL +
                    "'");
			//e.printStackTrace();
		}
	}
	/**
	 * 根据表名称创建表，如果已存在就先删除
	 * 
	 * @param tableName
	 * @param columnFamilies
	 */
	public void createTable(String tableName, String[] columnFamilies,
			int maxVersions)
	{
		deleteTable(tableName);
		try
		{
			Admin admin = conn.getAdmin();
			// 指定表名称
			HTableDescriptor descriptor = new HTableDescriptor(
					TableName.valueOf(tableName));
			// 添加列族
			for (String string : columnFamilies)
			{
				HColumnDescriptor family = new HColumnDescriptor(
						Bytes.toBytes(string));
				family.setMaxVersions(maxVersions);
				descriptor.addFamily(family);
			}
			admin.createTable(descriptor);

		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 根据表名称删除表
	 * 
	 * @param tableName
	 */
	public void deleteTable(String tableName)
	{
		try
		{
			Admin admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName)))
			{
				// 首先disable
				admin.disableTable(TableName.valueOf(tableName));
				// drop
				admin.deleteTable(TableName.valueOf(tableName));
			}

		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 根据表名获取表对象
	 */
	public Table getTable(String tableName)
	{
		try
		{
			return conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 根据计数器计算行健
	 * 
	 * @param tableGid
	 * @param familyGidGid
	 * @param qualifierGidActivityId
	 * @return
	 */
	public Long getId(String tableName, String family, String qualifier)
	{

		long num=0L;
		Table table = getTable(tableName);
		try
		{
			num=table.incrementColumnValue(
					Bytes.toBytes(ConstantsHBase.ROW_KEY_GID_ACTIVITY_ID),
					Bytes.toBytes(family), Bytes.toBytes(qualifier), 1);
			table.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return num;
	}

	/**
	 * rowKey为int，value为String
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family,
			String qualifier, String value)
	{
		Table table =getTable(tableName);
		Put put =new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try
		{
			table.put(put);
			table.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
	}
	
	/**
	 * rowKey为int，value为double
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void put(String tableName, Integer rowKey, String family,
			String qualifier, double value)
	{
		Table table =getTable(tableName);
		Put put =new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try
		{
			table.put(put);
			table.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public void put(String tableName, Integer rowKey, String family,
			String qualifier, int value)
	{
		Table table =getTable(tableName);
		Put put =new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try
		{
			table.put(put);
			table.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public void put(String tableName, String rowKey, String family,
			String qualifier, int value)
	{
		Table table =getTable(tableName);
		Put put =new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try
		{
			table.put(put);
			table.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public void put(String tableName, String rowKey, String family,
			String qualifier, String value)
	{
		Table table =getTable(tableName);
		Put put =new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try
		{
			table.put(put);
			table.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
