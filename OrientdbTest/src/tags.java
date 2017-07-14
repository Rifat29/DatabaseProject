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

import static java.lang.Class.forName;

/**
 * Created by rifat on 7/7/17.
 */
public class tags {
    public static void main(String[] args) {
        final long startTime = System.currentTimeMillis();
        Connection c = null;
        Statement stmt = null;
        try {
            forName("com.orientechnologies.orient.jdbc.OrientJdbcDriver");
            c =  DriverManager.getConnection("jdbc:orient:remote:localhost/StackOverflow", "admin","admin");
            System.out.println("Opened Database Successfully");
            c.setAutoCommit(false);

            stmt = c.createStatement();
            String sql = "CREATE CLASS Tags" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Tags.id integer" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Tags.tagName String" ;
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Tags.count integer";
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Tags.excerptPostId integer";
            stmt.executeUpdate(sql);
            sql =   " CREATE PROPERTY Tags.wikiPostId integer";
            stmt.executeUpdate(sql);

            System.out.println("class created successfully");

            File inputFile = new File("/media/rifat/New Volume/Tags.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            TagsHandler tagsHandler = new TagsHandler(c,stmt);
            saxParser.parse(inputFile, tagsHandler);

            c.commit();
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.out.println("Records created successfully" );
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
                String sql = "INSERT INTO Tags (id,tagName,count,excerptPostId,wikiPostId) "
                        + "VALUES ( "+id+", '"+tagName+"', "+count+", "+excerptPostId+", "+wikiPostId+");";
                stmt.executeUpdate(sql);

            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    @Override
    public void endDocument()
    {
        System.out.println("No of Rows: "+noOfRows);
    }
}
