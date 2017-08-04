package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ActivityDao
{
	public ActivityDao()
	{
		// activitySchema = parser.getTypes().get("oracle.avro.Activity");
		// activityBinding = catalog.getJsonBinding(activitySchema);
	}

	public List<MovieTO> getCustomerCurrentWatchList(int userId) throws IOException
	{
		List<MovieTO> movieTOs = new ArrayList<>();

		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
				CompareOp.EQUAL, Bytes.toBytes(5));
		/***
		 * 不是办法的办法：筛选所有浏览过的，把最后一个返回去
		 * oracle.demo.oow.bd.pojo.ActivityType
		 */
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

		try
		{
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			int id=0;
			while (iter.hasNext())
			{
				Result result = iter.next();
				id = Bytes.toInt(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
			}
			MovieTO movieTO = new MovieTO();
			MovieDao movieDao = new MovieDao();
			movieTO = movieDao
					.getMovieById(id);
			movieTOs.add(movieTO);
			

		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return movieTOs;
	}

	public ActivityTO getActivityTO(int userId, int movieId) throws IOException
	{
		ActivityTO activityTO = new ActivityTO();
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

		try
		{
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			while (iter.hasNext())
			{
				Result result = iter.next();
				activityTO.setCustId(userId);
				activityTO.setMovieId(movieId);
				if (result
						.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY)) != null)
				{
					activityTO
							.setActivity(ActivityType.getType(Bytes.toInt(result.getValue(
									Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
									Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY)))));
				}
				if (result
						.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID)) != null)
				{
					activityTO
							.setGenreId(Bytes.toInt(result.getValue(
									Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
									Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
				}
				if (result
						.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION)) != null)
				{
					activityTO
							.setPosition(Bytes.toInt(result.getValue(
									Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
									Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
				}
				if (result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE)) != null)
				{
					activityTO
							.setPrice(Bytes.toDouble(result.getValue(
									Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
									Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
				}
				if (result
						.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)) != null)
				{
					activityTO
							.setRating(RatingType.getType(Bytes.toInt(result.getValue(
									Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
									Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)))));
				}
				else
				{
					activityTO.setRating(RatingType.getType(1));
				}
				/*
				 * Bytes.toInt(result.getValue(
				 * Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				 * Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
				 */
				
			}

		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return activityTO;
	}

	public List<MovieTO> getCustomerBrowseList(int userId) throws IOException
	{
		List<MovieTO> movieTOs = new ArrayList<>();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
				CompareOp.EQUAL, Bytes.toBytes(5));
		/***
		 * 5代表了BROWSED_MOVIE状态的activity，参见Open Declaration
		 * oracle.demo.oow.bd.pojo.ActivityType
		 */
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

		try
		{
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			while (iter.hasNext())
			{
				MovieTO movieTO = new MovieTO();
				MovieDao movieDao = new MovieDao();
				Result result = iter.next();
				movieTO = movieDao
						.getMovieById(Bytes.toInt(result.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
				int num = 0;
				for (MovieTO movieTO2 : movieTOs)
				{
					if (movieTO.getId() == movieTO2.getId())
						num++;
				}
				if (num == 0)
					movieTOs.add(movieTO);
			}
			

		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return movieTOs;
	}

	public List<MovieTO> getCommonPlayList() throws IOException
	{
		MovieDao movieDao = new MovieDao();
		return movieDao.getRandomBySize(15);
	}

	public List<MovieTO> getCustomerHistoricWatchList(int userId) throws IOException
	{
		List<MovieTO> movieTOs = new ArrayList<>();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		Scan scan = new Scan();
		Filter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
				CompareOp.EQUAL, Bytes.toBytes(2));
		/***
		 * 2代表了COMPLETED_MOVIE状态的activity，参见Open Declaration
		 * oracle.demo.oow.bd.pojo.ActivityType
		 */
		((SingleColumnValueFilter) filter1).setFilterIfMissing(true);

		Filter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
				Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareOp.EQUAL, Bytes.toBytes(userId));

		((SingleColumnValueFilter) filter1).setFilterIfMissing(true);
		List<Filter> filters = new ArrayList<Filter>();
		//filters.add(filter1);
		filters.add(filter2);
		FilterList filterList1 = new FilterList(filters);
		scan.setFilter(filterList1);

		try
		{
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			while (iter.hasNext())
			{
				MovieTO movieTO = new MovieTO();
				MovieDao movieDao = new MovieDao();
				Result result = iter.next();
				movieTO = movieDao
						.getMovieById(Bytes.toInt(result.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
				int num = 0;
				for (MovieTO movieTO2 : movieTOs)
				{
					if (movieTO.getId() == movieTO2.getId())
						num++;
				}
				if (num == 0)
					movieTOs.add(movieTO);
			}
			

		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return movieTOs;
	}

	public void insertCustomerActivity(ActivityTO activityTO) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB
				.getTable(ConstantsHBase.TABLE_ACTIVITY);
		if (activityTO != null)
		{
			

			int movieId = activityTO.getMovieId();
			int userId = activityTO.getCustId();
			if (userId > 0 && movieId > 0)
			{
				FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal()
						.toString());
				try
				{

					

					Long id = hBaseDB.getId(ConstantsHBase.TABLE_GID,
							ConstantsHBase.FAMILY_GID_GID,
							ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);

					Put put = new Put(Bytes.toBytes(id));

					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
							Bytes.toBytes(activityTO.getCustId()));
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
							Bytes.toBytes(activityTO.getMovieId()));
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
							Bytes.toBytes(activityTO.getActivity().getValue()));//
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID),
							Bytes.toBytes(activityTO.getGenreId()));
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION),
							Bytes.toBytes(activityTO.getPosition()));
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE),
							Bytes.toBytes(activityTO.getPrice()));
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING),
							Bytes.toBytes(activityTO.getRating().getValue()));
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED),
							Bytes.toBytes(activityTO.isRecommended().getValue()));
					put.addColumn(
							Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
							Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME),
							Bytes.toBytes(activityTO.getTimeStamp()));

					table.put(put);
					
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		table.close();
	}

	public ActivityTO getActivityTOById(int aId) throws IOException
	{
		ActivityTO activityTO = new ActivityTO();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_ACTIVITY);
		try
		{
			Get get = new Get(Bytes.toBytes(aId));
			Result result = table.get(get);

			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID))!=null)
			{
				activityTO.setCustId(Bytes.toInt(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID))));
			}
			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))!=null)
			{
				activityTO.setMovieId(Bytes.toInt(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID))));
			}
			
			if (result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY)) != null)
			{
				activityTO
						.setActivity(ActivityType.getType(Bytes.toInt(result.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY)))));
			}
			if (result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID)) != null)
			{
				activityTO
						.setGenreId(Bytes.toInt(result.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
			}
			if (result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION)) != null)
			{
				activityTO
						.setPosition(Bytes.toInt(result.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
			}
			if (result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE)) != null)
			{
				activityTO
						.setPrice(Bytes.toDouble(result.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
			}
			if (result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)) != null)
			{
				activityTO
						.setRating(RatingType.getType(Bytes.toInt(result.getValue(
								Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),
								Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)))));
			}
			

		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return activityTO;
	}
}
