package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class GenreDao
{

	public void insertGenreMovie(MovieTO movieTO, GenreTO genreTO)
	{
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_GENRE,
				genreTO.getId() + "_" + movieTO.getId(),
				ConstantsHBase.FAMILY_GENRE_MOVIE,
				ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID, movieTO.getId());
		
	}

	public boolean isExist(GenreTO genreTO) throws IOException
	{
		HBaseDB db = HBaseDB.getInstance();
		Table table=db.getTable(ConstantsHBase.TABLE_GENRE);
		Get get = new Get(Bytes.toBytes(genreTO.getId()));
		get.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME));
		boolean flag=false;
		Result result;
		try
		{
			result = table.get(get);
			flag=!result.isEmpty();
		
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return flag;
	}

	public void insertGenre(GenreTO genreTO)
	{
		HBaseDB hBaseDB= HBaseDB.getInstance();
		hBaseDB.put(ConstantsHBase.TABLE_GENRE,genreTO.getId(),ConstantsHBase.FAMILY_GENRE_GENRE,ConstantsHBase.QUALIFIER_GENRE_NAME,genreTO.getName());
	}

	/**
	 * 获取genreMaxCount条分类信息，custId和movieMaxCount暂时无用
	 * @param custId
	 * @param movieMaxCount
	 * @param genreMaxCount
	 * @return
	 * @throws IOException 
	 */
	public List<GenreMovieTO> getMovies4Customer(int custId,int movieMaxCount,int genreMaxCount) throws IOException {
		List<GenreMovieTO> genreTOs = new ArrayList<>();
		Scan scan = new Scan();
		
		Filter filter = new PageFilter(genreMaxCount);
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE));
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
		//全表扫描
		try{
			ResultScanner resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			GenreTO genreTO = null;
			while(iter.hasNext())
			{
				genreTO = new GenreTO();
				Result result = iter.next();
				genreTO.setId(Bytes.toInt(result.getRow()));
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE),Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
				GenreMovieTO genreMovieTO = new GenreMovieTO();
				genreMovieTO.setGenreTO(genreTO);
				genreTOs.add(genreMovieTO);
			}
			
		}catch (IOException e) {
			e.printStackTrace();
		}
		table.close();
		return genreTOs;
	}
	
	public List<MovieTO> getMoviesByGenreId(int genreId,int movieCount) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(genreId + "_"));
		Filter filter2 = new PageFilter(movieCount);
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
		List<MovieTO> movieTOs = new ArrayList<>();
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
}
