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
public class comments {
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
            String sql = "CREATE TABLE comments " +
                    "(id       integer NOT NULL," +
                    " postId   integer NOT NULL," +
                    " score    integer  , " +
                    " text     text  , " +
                    " creationDate      timestamp,"+
                    " userId integer )";
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");

            File inputFile = new File("/media/rifat/New Volume/Comments.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            CommentsHandler userhandler = new CommentsHandler(c,stmt);
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

class CommentsHandler extends DefaultHandler {

    int noOfRows=0;
    String id,postId,score,text,creationDate,userId;
    Connection c = null;
    Statement stmt = null;

    public CommentsHandler(Connection connection,Statement statement)
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
            postId =attributes.getValue("PostId");
            score = attributes.getValue("Score");
            text = attributes.getValue("Text");
            creationDate =attributes.getValue("CreationDate");
            userId = attributes.getValue("UserId");
            noOfRows++;

            try {
                String sql = "INSERT INTO comments (id,postId,score,text,creationDate,userId) "
                        + "VALUES ( "+id+", "+postId+", "+score+", $rifat$ "+text+" $rifat$, '"+creationDate+"',"+userId+");";
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
