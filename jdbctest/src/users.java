import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by rifat on 7/6/17.
 */
public class users {
    public static void main(String[] args){
        final long startTime = System.currentTimeMillis();
        Connection c = null;
        Statement stmt = null;
        try {

            Class.forName("org.postgresql.Driver");
            c= DriverManager.getConnection("jdbc:postgresql://localhost:5432/StackOverflow","rifat","try.your.best.29");
            System.out.println("Opened Database Successfully");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            String sql = "CREATE TABLE users " +
                    "(id       integer PRIMARY KEY NOT NULL," +
                    " reputation   integer ," +
                    " creationDate      timestamp,"+
                    " displayName  text  NOT NULL, " +
                    " emailHash     text  , " +
                    " lastAccessDate     timestamp  , " +
                    " websiteUrl  text  , " +
                    " location    text  , " +
                    " age     integer  , " +
                    " aboutMe     text  , " +
                    " views     integer  , " +
                    " upVotes integer ,"+
                    " downVotes integer)";
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");

            File inputFile = new File("/media/rifat/New Volume/Users.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            UsersHandler userhandler = new UsersHandler(c,stmt);
            saxParser.parse(inputFile, userhandler);

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

            try {
                String sql = "INSERT INTO users (id,reputation,creationDate,displayName,emailHash,"
                        + "lastAccessDate,websiteUrl,location,age,aboutMe,views,upVotes,downVotes) "
                        + "VALUES ( "+id+", "+reputation+", '"+creationDate+"', $rifat$ "
                        +displayName+" $rifat$, $rifat$" +emailHash+"$rifat$,'"+lastAccessDate
                        +"',$rifat$"+websiteUrl+"$rifat$,$rifat$"+location+"$rifat$,"+age+",$rifat$"
                        +aboutMe+"$rifat$,"+views+","+upVotes+","+downVotes+");";
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("No of Rows: "+noOfRows);
                java.lang.System.exit(0);
            }
        }
    }

    @Override
    public void endDocument()
    {
        System.out.println("No of Rows: "+noOfRows);
    }
}

