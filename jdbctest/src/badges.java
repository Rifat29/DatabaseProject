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
 * Created by rifat on 7/5/17.
 */
public class badges {

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
            String sql = "CREATE TABLE badges " +
                    "(userId    integer NOT NULL," +
                    " name   text    NOT NULL, " +
                    " date      timestamp)";
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");

            File inputFile = new File("/media/rifat/New Volume/Badges.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            BadgesHandler userhandler = new BadgesHandler(c,stmt);
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

class BadgesHandler extends DefaultHandler {

    int noOfRows=0;
    String userId,name,dateTime;
    Connection c = null;
    Statement stmt = null;

    public BadgesHandler(Connection connection,Statement statement)
    {
        c=connection;
        stmt=statement;
    }

    @Override
    public void startElement(String uri,
                             String localName, String qName, Attributes attributes)
            throws SAXException {
        if (qName.equalsIgnoreCase("row")) {
            userId = attributes.getValue("UserId");
            name=attributes.getValue("Name");
            dateTime = attributes.getValue("Date");
            noOfRows++;

            try {
                String sql = "INSERT INTO badges (userId,name,date) "
                        + "VALUES ( "+userId+", '"+name+"', '"+dateTime+"');";
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


