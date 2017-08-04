package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CastDao
{

	public void insertCastInfo(CastTO castTO)
	{
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_CAST,
				castTO.getId(),
				ConstantsHBase.FAMILY_CAST_CAST,
				ConstantsHBase.QUALIFIER_CAST_NAME, castTO.getName());
		insertCastToMovie(castTO);
	}

	private void insertCastToMovie(CastTO castTO)
	{
		HBaseDB db = HBaseDB.getInstance();
		List<CastMovieTO> movieTOs=castTO.getCastMovieList();
		MovieDao movieDao=new MovieDao();
		for (CastMovieTO castMovieTO : movieTOs)
		{
			db.put(ConstantsHBase.TABLE_CAST,
					castTO.getId()+"_"+castMovieTO.getId(),
					ConstantsHBase.FAMILY_CAST_MOVIE,
					ConstantsHBase.QUALIFIER_CAST_MOVIE_ID, castMovieTO.getId());
			db.put(ConstantsHBase.TABLE_CAST,
					castTO.getId()+"_"+castMovieTO.getId(),
					ConstantsHBase.FAMILY_CAST_MOVIE,
					ConstantsHBase.QUALIFIER_CAST_CHARACTER, castMovieTO.getCharacter());
			db.put(ConstantsHBase.TABLE_CAST,
					castTO.getId()+"_"+castMovieTO.getId(),
					ConstantsHBase.FAMILY_CAST_MOVIE,
					ConstantsHBase.QUALIFIER_CAST_ORDER, castMovieTO.getOrder());
			movieDao.insertMovieCast(castTO,castMovieTO.getId());
		}
		
	}
	
	
	public CastTO getCastById(int crewId) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CAST);
		Get get = new Get(Bytes.toBytes(crewId));
		CastTO castTO = new CastTO();
		try
		{
			Result result = table.get(get);
			castTO.setId(crewId);
			castTO.setName(Bytes.toString(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));
			
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return castTO;
	}
	
	
	public List<MovieTO> getMoviesByCast(int castId) throws IOException 
	{
		List<MovieTO> movieTOs=new ArrayList<>();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CAST);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(castId + "_"));
		scan.setFilter(filter);

		ResultScanner resultScanner = null;
		try
		{
			resultScanner = table.getScanner(scan);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
		if (resultScanner != null)
		{
			Iterator<Result> iter = resultScanner.iterator();
			while (iter.hasNext())
			{
				Result result = iter.next();
				MovieTO movieTO = new MovieTO();
				MovieDao movieDao=new MovieDao();
				if (result != null & !result.isEmpty())
				{
					int movieId=Bytes.toInt(result.getValue(Bytes
							.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes
							.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID)));
					movieTO=movieDao.getMovieById(movieId);
					movieTOs.add(movieTO);
				}

			}
		}
		table.close();
		return movieTOs;
		
	}

}
