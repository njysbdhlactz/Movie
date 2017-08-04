package oracle.demo.oow.bd.ui;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import javax.servlet.*;
import javax.servlet.http.*;
import oracle.demo.oow.bd.dao.hbase.ActivityDao;
import oracle.demo.oow.bd.dao.hbase.UserDao;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;

public class logIn extends HttpServlet {
    //private static final String CONTENT_TYPE = "text/html; charset=windows-1252";
    private String loginPage = "login.jsp";
    private String indexPage = "index.jsp";

    @Override
	public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
	public void doGet(HttpServletRequest request,HttpServletResponse response) throws ServletException,IOException 
    {
        doPost(request, response);
    }

    @Override
	public void doPost(HttpServletRequest request,HttpServletResponse response) throws ServletException,IOException 
    {
        String message ="<h1>Please make sure your Hadoop Hbase Database instance is up and running</h1>";
        
        PrintWriter out = response.getWriter();
        try {
                String username = request.getParameter("username");
                String password = request.getParameter("password");
                boolean useMoviePosters = request.getParameter("useMoviePosters") == null? false : true;
                
                //CustomerDAO cdao = new CustomerDAO();
                UserDao userDao = new UserDao();
                CustomerTO cto =
                		userDao.getCustomerByCredential(username, password);
                Date date = new Date();

                if (cto != null) 
                {

                    // Delete all the previous ratings of the customers from the DB
                    //custRatingDAO.deleteCustomerRating(cto.getId());
                    
                    /////// ACTIVITY ////////
                    ActivityTO activityTO = new ActivityTO();
                    activityTO.setActivity(ActivityType.LOGIN);
                    activityTO.setCustId(cto.getId());
                    ActivityDao aDAO = new ActivityDao();
                    aDAO.insertCustomerActivity(activityTO);
                    
                    activityTO.setActivity(ActivityType.LIST_MOVIES);
                    aDAO.insertCustomerActivity(activityTO);
                    
                    HttpSession session = request.getSession();
                    session.setAttribute("username", username);
                    session.setAttribute("time", date);
                    session.setAttribute("userId", cto.getId());
                    session.setAttribute("name", cto.getName());
                    session.setAttribute("useMoviePosters", useMoviePosters);

                    //Ashok
                    System.out.println(" setting session and redirecting " + activityTO.toJsonString());
                    response.sendRedirect(indexPage);
                    
                } 
                else
                {
                    response.sendRedirect(loginPage + "?error=1");
                }
        } 
        catch (Exception e)
        {
           // e.printStackTrace();
        	 out.println(message);
        } //try/catch
    }
}
