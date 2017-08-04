package oracle.demo.oow.bd.util.hbase;

public class ConstantsHBase
{

	// 链接
	public static String HBSTORE_NAME = "rock";
	public static String HBSTORE_URL = "hdfs://rock:9000/hbase";

	public static String JDBC_URL = "jdbc:mysql://127.0.0.1:3306/movie?useUnicode=true&characterEncoding=utf-8";
    public static String MYSQL_DB_USER = "root";
    public static String MYSQL_DB_PASSWORD = "root";

	// 表名称
	public static final String TABLE_GID = "gid";
	public static final String TABLE_USER = "user";
	public static final String TABLE_GENRE = "genre";
	public static final String TABLE_MOVIE = "movie";
	public static final String TABLE_CAST = "cast";
	public static final String TABLE_CREW = "crew";
	public static final String TABLE_ACTIVITY = "activity";

	// 各个表中列族名称
	public static final String FAMILY_GID_GID = "gid";
	public static final String FAMILY_USER_ID = "id";
	public static final String FAMILY_USER_USER = "info";
	public static final String FAMILY_USER_GENRE = "genre";
	public static final String FAMILY_GENRE_GENRE = "genre";
	public static final String FAMILY_GENRE_MOVIE = "movie";
	public static final String FAMILY_MOVIE_MOVIE = "movie";
	public static final String FAMILY_MOVIE_GENRE = "genre";
	public static final String FAMILY_MOVIE_CAST = "cast";
	public static final String FAMILY_MOVIE_CREW = "crew";
	public static final String FAMILY_CAST_CAST = "cast";
	public static final String FAMILY_CAST_MOVIE = "movie";
	public static final String FAMILY_CREW_CREW = "crew";
	public static final String FAMILY_CREW_MOVIE = "movie";
	public static final String FAMILY_ACTIVITY_ACTIVITY = "activity";

	// gid表中行健名称，其他行健都是变量
	public static final String ROW_KEY_GID_ACTIVITY_ID = "activity_id";

	// 各个表中列名称
	// gid表
	public static final String QUALIFIER_GID_ACTIVITY_ID = "activity_id";
	// user表
	public static final String QUALIFIER_USER_ID = "id";
	public static final String QUALIFIER_USER_NAME = "name";
	public static final String QUALIFIER_USER_EMAIL = "email";
	public static final String QUALIFIER_USER_USERNAME = "username";
	public static final String QUALIFIER_USER_PASSWORD = "password";
	public static final String QUALIFIER_USER_GENRE_ID = "genre_id";
	public static final String QUALIFIER_USER_GENRE_NAME = "genre_name";
	public static final String QUALIFIER_USER_SCORE = "score";
	// genre表
	public static final String QUALIFIER_GENRE_NAME = "name";
	public static final String QUALIFIER_GENRE_MOVIE_ID = "movie_id";
	// movie
	public static final String QUALIFIER_MOVIE_ORIGINAL_TITLE = "original_title";
	public static final String QUALIFIER_MOVIE_OVERVIEW = "overview";
	public static final String QUALIFIER_MOVIE_POSTER_PATH = "poster_path";
	public static final String QUALIFIER_MOVIE_RELEASE_DATE = "release_date";
	public static final String QUALIFIER_MOVIE_VOTE_COUNT = "vote_count";
	public static final String QUALIFIER_MOVIE_RUNTIME = "runtime";
	public static final String QUALIFIER_MOVIE_POPULARITY = "popularity";
	public static final String QUALIFIER_MOVIE_GENRE_ID = "genre_id";
	public static final String QUALIFIER_MOVIE_GENRE_NAME = "genre_name";
	public static final String QUALIFIER_MOVIE_CAST_ID = "cast_id";
	public static final String QUALIFIER_MOVIE_CREW_ID = "crew_id";
	// cast表
	public static final String QUALIFIER_CAST_NAME = "name";
	public static final String QUALIFIER_CAST_MOVIE_ID = "movie_id";
	public static final String QUALIFIER_CAST_CHARACTER = "character";
	public static final String QUALIFIER_CAST_ORDER = "order";
	// crew表
	public static final String QUALIFIER_CREW_NAME = "name";
	public static final String QUALIFIER_CREW_JOB = "job";
	public static final String QUALIFIER_CREW_MOVIE_ID = "movie_id";
	// activity表
	public static final String QUALIFIER_ACTIVITY_USER_ID = "user_id";
	public static final String QUALIFIER_ACTIVITY_MOVIE_ID = "movie_id";
	public static final String QUALIFIER_ACTIVITY_GENRE_ID = "genre_id";
	public static final String QUALIFIER_ACTIVITY_ACTIVITY = "activity";
	public static final String QUALIFIER_ACTIVITY_RECOMMENDED = "recommended";
	public static final String QUALIFIER_ACTIVITY_TIME = "time";
	public static final String QUALIFIER_ACTIVITY_RATING = "rating";
	public static final String QUALIFIER_ACTIVITY_PRICE = "price";
	public static final String QUALIFIER_ACTIVITY_POSITION = "position";

}
