package oracle.demo.oow.bd.util.hbase;

/**
 * 初始化表
 * 
 * @author Administrator
 * 
 */
public class InitHBase
{

	public static void main(String[] args) throws Exception
	{
		HBaseDB db = HBaseDB.getInstance();
		// gid
		/*System.out.println("创建gid");
		String table_gid = ConstantsHBase.TABLE_GID;
		String[] fam_gid = { ConstantsHBase.FAMILY_GID_GID };
		db.createTable(table_gid, fam_gid, 1);
		System.out.println("创建gid完成");
		// user
		System.out.println("创建user");
		String table_user = ConstantsHBase.TABLE_USER;
		String[] fam_user = { ConstantsHBase.FAMILY_USER_ID,
				ConstantsHBase.FAMILY_USER_USER,
				ConstantsHBase.FAMILY_USER_GENRE };
		db.createTable(table_user, fam_user, 1);
		System.out.println("创建user完成");
		// genre
		System.out.println("创建genre");
		String table_genre = ConstantsHBase.TABLE_GENRE;
		String[] fam_genre = { ConstantsHBase.FAMILY_GENRE_GENRE,
				ConstantsHBase.FAMILY_GENRE_MOVIE };
		db.createTable(table_genre, fam_genre, 1);
		System.out.println("创建genre完成");
		// movie
		System.out.println("创建movie");
		String table_movie = ConstantsHBase.TABLE_MOVIE;
		String[] fam_movie = { ConstantsHBase.FAMILY_MOVIE_CAST,
				ConstantsHBase.FAMILY_MOVIE_CREW,
				ConstantsHBase.FAMILY_MOVIE_GENRE,
				ConstantsHBase.FAMILY_MOVIE_MOVIE };
		db.createTable(table_movie, fam_movie, 1);
		System.out.println("创建movie完成");

		// cast
		System.out.println("创建cast");
		String table_cast = ConstantsHBase.TABLE_CAST;
		String[] fam_cast = { ConstantsHBase.FAMILY_CAST_CAST,
				ConstantsHBase.FAMILY_CAST_MOVIE };
		db.createTable(table_cast, fam_cast, 1);
		System.out.println("创建cast完成");

		// crew
		System.out.println("创建crew");
		String table_crew = ConstantsHBase.TABLE_CREW;
		String[] fam_crew = { ConstantsHBase.FAMILY_CREW_CREW,
				ConstantsHBase.FAMILY_CREW_MOVIE };
		db.createTable(table_crew, fam_crew, 1);
		System.out.println("创建crew完成");

		// activity
		System.out.println("创建activity");
		String table_activity = ConstantsHBase.TABLE_ACTIVITY;
		String[] fam_activity = { ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY };
		db.createTable(table_activity, fam_activity, 1);
		System.out.println("创建activity完成");*/
		System.out.println("创建activity");
		String table_activity = ConstantsHBase.TABLE_ACTIVITY;
		String[] fam_activity = { ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY };
		db.createTable(table_activity, fam_activity, 1);
		System.out.println("创建activity完成");

	}
}
