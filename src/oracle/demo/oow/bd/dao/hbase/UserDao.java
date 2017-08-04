package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class UserDao
{

	

	/**
	 * 根据分类id获取电影信息
	 * 
	 * @param custId
	 * @param genreId
	 * @return
	 */
	public int MOVIE_MAX_COUNT = 25;
	public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(genreId + "_"));
		Filter filter2 = new PageFilter(MOVIE_MAX_COUNT);
		FilterList filterList = new FilterList(filter, filter2);
		scan.setFilter(filterList);

		ResultScanner resultScanner = null;
		try
		{
			resultScanner = table.getScanner(scan);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		List<MovieTO> movieTOs = new ArrayList();
		MovieTO movieTO = null;
		if (resultScanner != null)
		{
			Iterator<Result> iter = resultScanner.iterator();
			MovieDao movieDao = new MovieDao();
			while (iter.hasNext())
			{
				Result result = iter.next();
				if (result != null & !result.isEmpty())
				{
					int movieId = Bytes.toInt(result.getValue(Bytes
							.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes
							.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID)));
					movieTO = movieDao.getMovieById(movieId);

					if (StringUtil.isNotEmpty(movieTO.getPosterPath()))
					{
						movieTO.setOrder(100);
					} else
					{
						movieTO.setOrder(0);
					}
					movieTOs.add(movieTO);
				}

			}
		}
		table.close();
		return movieTOs;
	}

	public void insert(CustomerTO customerTO) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable("user");
		if (table != null)
		{
			Put put1 = new Put(Bytes.toBytes(customerTO.getUserName()));
			// username-->id的映射
			put1.addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"),
					Bytes.toBytes(customerTO.getId()));
			// 用户的基本信息
			Put put2 = new Put(Bytes.toBytes(customerTO.getId()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
					Bytes.toBytes(customerTO.getName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"),
					Bytes.toBytes(customerTO.getEmail()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"),
					Bytes.toBytes(customerTO.getUserName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"),
					Bytes.toBytes(customerTO.getPassword()));

			List<Put> puts = new ArrayList();
			puts.add(put1);
			puts.add(put2);

			try
			{
				table.put(puts);
				
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		table.close();
	}

	public CustomerTO getCustomerByCredential(String username, String password)
	{
		CustomerTO customerTO = null;
		// 首先通过userName查询ID
		try
		{
			int id = getIdByUserName(username);
			if (id > 0)
			{
				customerTO = getInfoById(id);
				if (customerTO != null)
				{
					if (!customerTO.getPassword().equals(password))
					{
						customerTO = null;
					}
				}
			}
		} catch (Exception e)
		{
			//e.printStackTrace();
			return customerTO;
		}

		// 再通过id查询用户信息
		return customerTO;
	}

	private CustomerTO getInfoById(int id) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable("user");
		Get get = new Get(Bytes.toBytes(id));
		CustomerTO customerTO = new CustomerTO();
		try
		{
			Result result = table.get(get);
			customerTO.setEmail(Bytes.toString(result.getValue(
					Bytes.toBytes("info"), Bytes.toBytes("email"))));
			customerTO.setId(id);
			customerTO.setName(Bytes.toString(result.getValue(
					Bytes.toBytes("info"), Bytes.toBytes("name"))));
			customerTO.setPassword(Bytes.toString(result.getValue(
					Bytes.toBytes("info"), Bytes.toBytes("password"))));
			customerTO.setUserName(Bytes.toString(result.getValue(
					Bytes.toBytes("info"), Bytes.toBytes("username"))));

		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		table.close();
		return customerTO;
	}

	public int getIdByUserName(String username) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable("user");

		Get get = new Get(Bytes.toBytes(username));
		int id = 0;
		try
		{
			Result result = table.get(get);
			id = Bytes.toInt(result.getValue(Bytes.toBytes("id"),
					Bytes.toBytes("id")));
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		table.close();
		return id;
	}

}
