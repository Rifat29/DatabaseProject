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
 * Created by rifat on 7/13/17.
 */
public class tags {
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
            String sql = "CREATE TABLE tags " +
                    "(id integer PRIMARY KEY     NOT NULL," +
                    " tagName   text    NOT NULL, " +
                    " count            integer     NOT NULL, " +
                    " excerptPostId    integer, " +
                    " wikiPostId       integer)";
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");

            File inputFile = new File("/media/rifat/New Volume/Tags.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            TagsHandler userhandler = new TagsHandler(c,stmt);
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

class TagsHandler extends DefaultHandler {

    int noOfRows=0;
    String id,tagName,count,excerptPostId,wikiPostId;
    Connection c = null;
    Statement stmt = null;

    public TagsHandler(Connection connection,Statement statement)
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
            tagName=attributes.getValue("TagName");
            count = attributes.getValue("Count");
            excerptPostId = attributes.getValue("ExcerptPostId");
            wikiPostId = attributes.getValue("WikiPostId");
            noOfRows++;

            try {
                String sql = "INSERT INTO tags (id,tagName,count,excerptPostId,wikiPostId) "
                        + "VALUES ( "+id+", '"+tagName+"', "+count+", "+excerptPostId+", "+wikiPostId+");";
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public void endDocument()
    {
        System.out.println("No of Rows: "+noOfRows);
    }
}