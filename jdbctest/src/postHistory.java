import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.XMLConstants;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by rifat on 7/8/17.
 */
public class postHistory {
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
            String sql = "CREATE TABLE postHistory " +
                    "(id   integer NOT NULL," +
                    " postHistoryTypeId   integer NOT NULL," +
                    " postId      integer NOT NULL,"+
                    " revisionGUID  text , " +
                    " creationDate     timestamp  , " +
                    " userId integer , " +
                    " userDisplayName    text  , " +
                    " comment     text  , " +
                    " text    text  , " +
                    " closeReasonId integer )";
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");

            File inputFile = new File("/media/rifat/New Volume/PostHistory.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING,false);
            SAXParser saxParser = factory.newSAXParser();
            PostHistoryHandler userhandler = new PostHistoryHandler(c,stmt);
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

class PostHistoryHandler extends DefaultHandler {

    int noOfRows=0;
    String id,postHistoryTypeId,postId,revisionGUID,creationDate,userId,userDisplayName,comment;
    String text,closeReasonId;
    Connection c = null;
    Statement stmt = null;

    public PostHistoryHandler(Connection connection,Statement statement)
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
            postHistoryTypeId =attributes.getValue("PostHistoryTypeId");
            postId  = attributes.getValue("PostId");
            revisionGUID  = attributes.getValue("RevisionGUID");
            creationDate =attributes.getValue("CreationDate");
            userId = attributes.getValue("UserId");
            userDisplayName = attributes.getValue("UserDisplayName");
            comment  = attributes.getValue("Comment");
            text = attributes.getValue("Text");
            closeReasonId = attributes.getValue("CloseReasonId");
            noOfRows++;

            try {
                String sql = "INSERT INTO postHistory (id,postHistoryTypeId,postId,revisionGUID,"
                        + "creationDate,userId,userDisplayName,comment,text,closeReasonId) "
                        + "VALUES ( "+id+", "+postHistoryTypeId+","+postId+", $rifat$ "
                        +revisionGUID+" $rifat$,'" +creationDate+"',"+userId
                        +",$rifat$"+userDisplayName+"$rifat$,$rifat$"+comment+"$rifat$,$rifat$"
                        +text+"$rifat$,"+closeReasonId+");";
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
