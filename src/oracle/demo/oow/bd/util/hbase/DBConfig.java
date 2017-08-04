package oracle.demo.oow.bd.util.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import oracle.demo.oow.bd.util.FileWriterUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConfig extends HttpServlet
{

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DBConfig.class);
	private static final String ACTIVITY_FILENAME = "conf.properties";

	@Override
	public void init(ServletConfig config) throws ServletException
	{
		super.init(config);

		Properties properties = new Properties();

		InputStream inputStream = null;
		try
		{
			inputStream = DBConfig.class.getClassLoader().getResourceAsStream(
					ACTIVITY_FILENAME);
			properties.load(inputStream);
			Set<Object> proKeySet = properties.keySet();
			Iterator<Object> it = proKeySet.iterator();
			while (it.hasNext())
			{
				String key = it.next().toString();
				String val = properties.getProperty(key);

				setPorperty(key, val);

			}
		} catch (IOException e)
		{
			e.printStackTrace();
			LOGGER.error("读取资源文件" + ACTIVITY_FILENAME + "失败", e);
			throw new RuntimeException("读取资源文件" + ACTIVITY_FILENAME + "失败");
		} finally
		{
			if (inputStream != null)
			{
				try
				{
					inputStream.close();
				} catch (IOException e)
				{
					e.printStackTrace();
					LOGGER.error("关闭流失败", e);
				}
			}

		}

	}

	private static void setPorperty(String key, String val)
	{

		if ("HBSTORE_NAME".equals(key))
		{
			ConstantsHBase.HBSTORE_NAME = val;
		}

		if ("HBSTORE_URL".equals(key))
		{
			ConstantsHBase.HBSTORE_URL = val;
		}

		if ("OUTPUT_FILE".equals(key))
		{
			FileWriterUtil.OUTPUT_FILE = val;
		}

		if ("JDBC_URL".equals(key))
		{
			ConstantsHBase.JDBC_URL = val;
		}
		if ("MYSQL_DB_USER".equals(key))
		{
			ConstantsHBase.MYSQL_DB_USER = val;
		}
		if ("MYSQL_DB_PASSWORD".equals(key))
		{
			ConstantsHBase.MYSQL_DB_PASSWORD = val;
		}
	}

}