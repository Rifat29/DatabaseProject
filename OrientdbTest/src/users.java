import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.sql.*;

import static java.lang.Class.forName;

/**
 * Created by rifat on 7/11/17.
 */
public class users {
    public static void main(String[] args){
        final long startTime = System.currentTimeMillis();
        Connection c = null;
        Statement stmt = null;
        try {

            forName("com.orientechnologies.orient.jdbc.OrientJdbcDriver");
            c =  DriverManager.getConnection("jdbc:orient:remote:localhost/StackOverflow", "admin","admin");
            System.out.println("Opened Database Successfully");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            String sql = "CREATE CLASS Users";
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.id integer" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.reputation   integer" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.creationDate datetime";
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.displayName string" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.emailHash  string" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.lastAccessDate datetime";
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.websiteUrl string" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.location  string" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.age  integer" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.aboutMe  string" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.views integer" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.upVotes integer" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Users.downVotes integer" ;
            stmt.executeUpdate(sql);

            System.out.println("Class created successfully");

            File inputFile = new File("/media/rifat/New Volume1/Users.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            UsersHandler usersHandler = new UsersHandler(c,stmt);
            saxParser.parse(inputFile, usersHandler);

            stmt.close();
            c.commit();
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Records created successfully");
        final long endTime = System.currentTimeMillis();

        System.out.println("Total execution time: " + (endTime - startTime) );
    }
}

class UsersHandler extends DefaultHandler {

    int noOfRows=0;
    String id,reputation,creationDate,displayName,emailHash,lastAccessDate,websiteUrl,location,age;
    String aboutMe,views,upVotes,downVotes;
    Connection c = null;
    Statement stmt = null;

    public UsersHandler(Connection connection,Statement statement)
    {
        c=connection;
        stmt=statement;
    }

    @Override
    public void startElement(String uri,
                             String localName, String qName, Attributes attributes)
            throws SAXException {
        if (qName.equalsIgnoreCase("row")) {
            id = attributes.getValue("Id");
            reputation =attributes.getValue("Reputation");
            creationDate =attributes.getValue("CreationDate");
            displayName = attributes.getValue("DisplayName");
            emailHash  = attributes.getValue("EmailHash");
            lastAccessDate = attributes.getValue("LastAccessDate");
            websiteUrl  = attributes.getValue("WebsiteUrl");
            location = attributes.getValue("Location");
            age = attributes.getValue("Age");
            aboutMe = attributes.getValue("AboutMe");
            views = attributes.getValue("Views");
            upVotes  = attributes.getValue("UpVotes");
            downVotes = attributes.getValue("DownVotes");
            noOfRows++;

            if (reputation==null)
            {
                reputation="0";
            }
            if (age==null)
            {
                age="0";
            }


            try {
                String sql = "INSERT INTO Users (id,reputation,creationDate,displayName,emailHash,"
                        + "lastAccessDate,websiteUrl,location,age,aboutMe,views,upVotes,downVotes) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

                PreparedStatement preparedStatement = c.prepareStatement(sql);
                preparedStatement.setInt(1, Integer.parseInt(id));
                preparedStatement.setInt(2, Integer.parseInt(reputation));
                preparedStatement.setString(3, creationDate);
                preparedStatement.setString(4,displayName);
                preparedStatement.setString(5,emailHash);
                preparedStatement.setString(6,lastAccessDate);
                preparedStatement.setString(7,websiteUrl);
                preparedStatement.setString(8,location);
                preparedStatement.setInt(9,Integer.parseInt(age));
                preparedStatement.setString(10,aboutMe);
                preparedStatement.setInt(11,Integer.parseInt(views));
                preparedStatement.setInt(12,Integer.parseInt(upVotes));
                preparedStatement.setInt(13,Integer.parseInt(downVotes));

                preparedStatement .executeUpdate();

            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("ERROR at Row: "+noOfRows);
            }
        }
    }

    @Override
    public void endDocument()
    {
        System.out.println("No of Rows: "+noOfRows);
    }
}