package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.to.CrewTO;
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

public class CrewDao
{

	public void insertCrewInfo(CrewTO crewTO)
	{
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_CREW,
				crewTO.getId(),
				ConstantsHBase.FAMILY_CREW_CREW,
				ConstantsHBase.QUALIFIER_CREW_NAME, crewTO.getName());
		db.put(ConstantsHBase.TABLE_CREW,
				crewTO.getId(),
				ConstantsHBase.FAMILY_CREW_CREW,
				ConstantsHBase.QUALIFIER_CREW_JOB, crewTO.getJob());
		insertCrewToMovie(crewTO);
		
	}

	private void insertCrewToMovie(CrewTO crewTO)
	{
		HBaseDB db = HBaseDB.getInstance();
		List<String> movieTOs=crewTO.getMovieList();
		MovieDao movieDao=new MovieDao();
		for (String movieId : movieTOs)
		{
			db.put(ConstantsHBase.TABLE_CREW,
					crewTO.getId()+"_"+movieId,
					ConstantsHBase.FAMILY_CREW_MOVIE,
					ConstantsHBase.QUALIFIER_CREW_MOVIE_ID, movieId);
			movieDao.insertMovieCrew(crewTO,Integer.valueOf(movieId));
		}
		
	}

	public CrewTO getCrewById(int crewId) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CREW);
		Get get = new Get(Bytes.toBytes(crewId));
		CrewTO crewTO = new CrewTO();
		try
		{
			Result result = table.get(get);
			crewTO.setId(crewId);
			crewTO.setJob(Bytes.toString(result.getValue(Bytes
					.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes
					.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));
			crewTO.setName(Bytes.toString(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));
			
			
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return crewTO;
	}
	
	public List<MovieTO> getMoviesByCrew(int crewId) throws IOException 
	{
		List<MovieTO> movieTOs=new ArrayList<>();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CREW);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(crewId + "_"));
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
					int movieId=Integer.valueOf(Bytes.toString(result.getValue(Bytes
							.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE), Bytes
							.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID))));
					
					//System.out.println("zxc!!"+movieId);
					movieTO=movieDao.getMovieById(movieId);
					//System.out.println("zxc!!"+movieTO.toJsonString());
					movieTOs.add(movieTO);
				}

			}
		}
		table.close();
		return movieTOs;
		
	}
	
	
}
