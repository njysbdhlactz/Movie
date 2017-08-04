package oracle.demo.oow.bd.util.hbase.loader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CastDao;
import oracle.demo.oow.bd.dao.hbase.CrewDao;
import oracle.demo.oow.bd.dao.hbase.MovieDao;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;

public class MovieLoader
{

	/**
	 * To start loading the data, you need to just run this class. No additional
	 * input arguments are required to run this class.
	 * 
	 * @param args
	 */
	public static void main(String[] args)
	{

		try
		{
			MovieLoader loader =new MovieLoader();
			//loader.uploadProfile();
			loader.uploadMovieCast();
			loader.uploadMovieCrew();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public void uploadProfile() throws IOException
	{
		FileReader fr = null;
		try
		{
			fr = new FileReader(Constant.MOVIE_INFO_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			MovieTO movieTo=new MovieTO();
			MovieDao movieDao=new MovieDao();
			int count = 1;
			while ((jsonTxt = br.readLine()) != null)
			{

				if (jsonTxt.trim().length() == 0)
					continue;

				try
				{
					movieTo = new MovieTO(jsonTxt.trim());
				} catch (Exception e)
				{
					System.out.println("ERROR: Not able to parse the json string: \t"
									+ jsonTxt);
				}

				if (movieTo != null)
				{
					movieDao.insertMovie(movieTo);
					System.out.println(count++ + " " + movieTo.getMovieJsonTxt());

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} finally
		{
			fr.close();
		}
	} // uploadProfile
	
	
	public void uploadMovieCast() throws IOException
	{
		FileReader fr = null;
		try
		{
			fr = new FileReader(Constant.MOVIE_CASTS_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			CastTO castTO=new CastTO();
			int count = 1;
			while ((jsonTxt = br.readLine()) != null)
			{

				if (jsonTxt.trim().length() == 0)
					continue;

				try
				{
					castTO = new CastTO(jsonTxt.trim());
				} catch (Exception e)
				{
					System.out.println("ERROR: Not able to parse the json string: \t"
									+ jsonTxt);
				}

				if (castTO != null)
				{
					CastDao castDao= new CastDao();
					castDao.insertCastInfo(castTO);
					System.out.println(count++ + " " + castTO.getJsonTxt());

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} finally
		{
			fr.close();
		}
	} // uploadMovieCast
	
	public void uploadMovieCrew() throws IOException
	{
		FileReader fr = null;
		try
		{
			fr = new FileReader(Constant.MOVIE_CREW_FILE_NAME);
			BufferedReader br = new BufferedReader(fr);
			String jsonTxt = null;
			CrewTO crewTO=new CrewTO();
			int count = 1;
			while ((jsonTxt = br.readLine()) != null)
			{

				if (jsonTxt.trim().length() == 0)
					continue;

				try
				{
					crewTO = new CrewTO(jsonTxt.trim());
				} catch (Exception e)
				{
					System.out.println("ERROR: Not able to parse the json string: \t"
									+ jsonTxt);
				}

				if (crewTO != null)
				{
					CrewDao crewDao= new CrewDao();
					crewDao.insertCrewInfo(crewTO);
					System.out.println(count++ + " " + crewTO.getJsonTxt());

				} // EOF if

			} // EOF while
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} finally
		{
			fr.close();
		}
	} 

}
