package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.hbase.HBaseDB;

public class MovieDao
{

	public void insertMovie(MovieTO movieTO) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE,
				movieTO.getTitle());
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW, movieTO.getOverview());
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH,
				movieTO.getPosterPath());
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE,
				movieTO.getReleasedYear());
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT,
				movieTO.getVoteCount());
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_RUNTIME, movieTO.getRunTime());
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_MOVIE,
				ConstantsHBase.QUALIFIER_MOVIE_POPULARITY,
				movieTO.getPopularity());

		insertMovieGenres(movieTO);
	}

	private void insertMovieGenres(MovieTO movieTO) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		List<GenreTO> genreTOs = movieTO.getGenres();
		GenreDao genreDao = new GenreDao();
		for (GenreTO genreTO : genreTOs)
		{
			hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId() + "_"
					+ genreTO.getId(), ConstantsHBase.FAMILY_MOVIE_GENRE,
					ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID, genreTO.getId());
			hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId() + "_"
					+ genreTO.getId(), ConstantsHBase.FAMILY_MOVIE_GENRE,
					ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME,
					genreTO.getName());
			if (!genreDao.isExist(genreTO))
			{
				genreDao.insertGenre(genreTO);
			}
			genreDao.insertGenreMovie(movieTO, genreTO);
		}
	}

	public void insertMovieCast(CastTO castTO, int movieId)
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieId + "_" + castTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_CAST,
				ConstantsHBase.QUALIFIER_MOVIE_CAST_ID, castTO.getId());
	}

	public void insertMovieCrew(CrewTO crewTO, int movieId)
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		hBaseDB.put(ConstantsHBase.TABLE_MOVIE, movieId + "_" + crewTO.getId(),
				ConstantsHBase.FAMILY_MOVIE_CREW,
				ConstantsHBase.QUALIFIER_MOVIE_CREW_ID, crewTO.getId());
	}

	public MovieTO getMovieById(int movieId) throws IOException
	{
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Get get = new Get(Bytes.toBytes(movieId));
		MovieTO movieTO = new MovieTO();
		try
		{
			Result result = table.get(get);
			movieTO.setId(movieId);
			movieTO.setTitle(Bytes.toString(result.getValue(Bytes
					.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes
					.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))!=null)
			{
				movieTO.setOverview(Bytes.toString(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
			}
			
			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))!=null)
			{
				movieTO.setPosterPath(Bytes.toString(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))));
			}
			
			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))!=null)
			{
				movieTO.setDate(Bytes.toString(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))));
			}
			
			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))!=null)
			{
				movieTO.setVoteCount(Bytes.toInt(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
			}
			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))!=null)
			{
				movieTO.setRunTime(Bytes.toInt(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
			}
			if(result.getValue(
					Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
					Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))!=null)
			{
				movieTO.setPopularity(Bytes.toDouble(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
			}
			
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		table.close();
		return movieTO;
	}

	public MovieTO getMovieDetailById(int movieId) throws IOException
	{
		MovieTO movieTO = getMovieById(movieId);
		ArrayList<GenreTO> genres = getGenresByMovieId(movieId);
		// 设置genre
		movieTO.setGenres(genres);
		CastCrewTO castCrewTO = new CastCrewTO();
		// 设置Cast
		List<CrewTO> crewTOs = getCrewsByMovieId(movieId);
		castCrewTO.setCrewList(crewTOs);

		movieTO.setCastCrewTO(castCrewTO);
		return movieTO;
	}

	private List<CrewTO> getCrewsByMovieId(int movieId) throws IOException
	{
		List<CrewTO> crewTOs = new ArrayList<>();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId + "_"));
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
				CrewTO crewTO = null;
				CrewDao crewDao=new CrewDao();
				if (result != null & !result.isEmpty())
				{
					int crewId = Bytes.toInt(result.getValue(Bytes
							.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW), Bytes
							.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CREW_ID)));
					crewTO = crewDao.getCrewById(crewId);

					crewTOs.add(crewTO);
				}

			}
		}
		table.close();
		return crewTOs;
	}

	private ArrayList<GenreTO> getGenresByMovieId(int movieId) throws IOException
	{
		ArrayList<GenreTO> genreTOs=new ArrayList<>();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId + "_"));
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
				GenreTO genreTO = new GenreTO();
				if (result != null & !result.isEmpty())
				{
					genreTO.setId(Bytes.toInt(result.getValue(Bytes
							.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes
							.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID))));
					genreTO.setName(Bytes.toString(result.getValue(Bytes
							.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes
							.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME))));
					genreTOs.add(genreTO);
				}

			}
		}
		table.close();
		return genreTOs;
	}

	/**
	 * 随机过滤器
	 * 
	 * @param movieId
	 * @return
	 * @throws IOException
	 */
	public List<MovieTO> getRandomBySize(int maxNum) throws IOException
	{
		List<MovieTO> movieTOs =new ArrayList<>();
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Filter filter = new RandomRowFilter(0.5f);
		// System.out.println("Results of scan:");

		try
		{
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			int rowCount = 0;
			for (Result result : scanner) {
				//System.out.println(result);
				MovieTO movieTO=new MovieTO();
				movieTO.setId(Bytes.toInt(result.getRow()));
				movieTO.setTitle(Bytes.toString(result.getValue(Bytes
						.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes
						.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
				movieTO.setOverview(Bytes.toString(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
				movieTO.setPosterPath(Bytes.toString(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH))));
				movieTO.setDate(Bytes.toString(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))));
				movieTO.setVoteCount(Bytes.toInt(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
				movieTO.setRunTime(Bytes.toInt(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
				movieTO.setPopularity(Bytes.toDouble(result.getValue(
						Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE),
						Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
				movieTOs.add(movieTO);
				rowCount++;
				if(rowCount>=maxNum)
					break;
			}

		} catch (IOException e)
		{
			e.printStackTrace();
		}

		table.close();
		return movieTOs;
	}

}
