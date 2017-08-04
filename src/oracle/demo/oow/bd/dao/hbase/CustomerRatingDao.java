package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class CustomerRatingDao
{

	public void insertCustomerRating(int userId, int movieId, int rating) throws IOException
	{
		

		insertCustomerRating2(userId, movieId, rating);
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
				CompareOp.EQUAL, Bytes.toBytes(movieId));
		((SingleColumnValueFilter) filter1).setFilterIfMissing(true);

		Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareOp.EQUAL, Bytes.toBytes(userId));

		((SingleColumnValueFilter) filter1).setFilterIfMissing(true);
		List<Filter> filters = new ArrayList<Filter>();
		filters.add(filter1);
		filters.add(filter2);
		FilterList filterList1 = new FilterList(filters);
		scan.setFilter(filterList1);

		ActivityDao activityDao=new ActivityDao();
		try
		{
			ResultScanner resultScanner = table.getScanner(scan);
			if (resultScanner == null)
			{
				// 新增
				ActivityType activityType = ActivityType.RATE_MOVIE;

				Long id = hBaseDB.getId(ConstantsHBase.TABLE_GID,
						ConstantsHBase.FAMILY_GID_GID,
						ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);

				Table table2 = hBaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);

				Put put = new Put(Bytes.toBytes(id));

				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
						Bytes.toBytes(movieId));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
						Bytes.toBytes(activityType.getValue()));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING),
						Bytes.toBytes(rating));
				put.addColumn(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
						Bytes.toBytes(userId));
				
				table2.put(put);
				table2.close();
			} else
			{
				Iterator<Result> iter = resultScanner.iterator();
				while (iter.hasNext())
				{
					Result result = iter.next();
					// 更新

					
					ActivityTO activityTO=activityDao.getActivityTOById(Bytes.toInt(result.getRow()));
					if(activityTO.getMovieId()>0&&activityTO.getCustId()>0)
					{
						FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal()
								.toString());
					}
					Table table2 = hBaseDB
							.getTable(ConstantsHBase.TABLE_ACTIVITY);

					Put put = new Put(result.getRow());

					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING),
							Bytes.toBytes(rating));
					
					table2.close();
					table2.put(put);
				}
				
			}

			
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
		table.close();

	}

	
	/**
	 * 从当前观看列表里获取一个电影，获取其分类，再从分类里找几个
	 * @throws IOException 
	 * 
	 * */
	
	public List<MovieTO> getMoviesByMood(int userId) throws IOException
	{
		MovieDao movieDao = new MovieDao();
		List<MovieTO> movieList =  new ArrayList<>();
		MovieTO movieTO=new MovieTO();
		String s1="SELECT * from recommend WHERE userid="+userId+" ORDER BY weights desc";
		try
		{
			Connection conn = BaseDao.getOraConnect();
			ResultSet rs = BaseDao.executeQuery(s1);
			while(rs.next())
			{
				int id=Integer.valueOf(rs.getString("movieid"));
				System.out.println("xzczxc"+id);
				movieTO=movieDao.getMovieById(id);
				movieList.add(movieTO);
				
			}
		} catch (Exception e)
		{
			System.out.println("数据库连接失败！");
			e.printStackTrace();
		}
		
		//System.out.println("asddsdasdad"+movieList.size());
		return movieList;
	}
	
	
	public void insertCustomerRating2(int userId, int movieId, int rating)
	{
		try
		{
			Connection conn = BaseDao.getOraConnect();
			PreparedStatement pstmt = conn
					.prepareStatement("insert into cust_rating(userid,movieid,rating) values(" +
							userId+"," +
							movieId	+"," +
							rating+")");
			try
			{
				pstmt.executeUpdate();
			} catch (Exception e)
			{
				System.out.println("数据写入失败！");
				e.printStackTrace();
			}
			//System.out.println(pstmt.toString());
		} catch (Exception e)
		{
			System.out.println("数据库连接失败！");
			e.printStackTrace();
		}
		
		
		
	}
		
}
