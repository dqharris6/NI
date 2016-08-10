package pmi_scraper;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class PMI_Scraper
{
	public static void main(String args[]) throws IOException, ParseException
	{   
            String[] countries      = {"united-states", "china", "japan", "france", "germany", "italy", "united-kingdom", "spain"};  // list of coutries that we want PMI values from
            
            SimpleDateFormat sdf    = new SimpleDateFormat("dd/MM/yyyy");       // creates an instance of simple date
            Date test_date          = sdf.parse("21/12/2012");                  // sets date other than today (testing purposes), fill in dd/MM/yyyy above to change date
            
            Calendar today          = Calendar.getInstance();
            String curr_month       = new SimpleDateFormat("MMMMMMMMM").format(today.getTime());    // Use test_date to test a specific day
            String curr_year        = new SimpleDateFormat("YYYY").format(today.getTime());         // Use today to generate today's results
            
            String curr_month_PMI   = curr_month + " PMI";  // adds formatting to txt file output
            
            Formatter fmtFile       = new Formatter(new FileOutputStream("PMI_" + curr_month + curr_year + ".txt"));    // creates new formatter instance, creates file name with current month and year
            fmtFile.format("%15s %15s %15s %n","Country",curr_month_PMI,"Previous PMI");                                // formats txt file

            for(int countriesVar = 0; countriesVar<countries.length; ++countriesVar) // loops through countries array, does the following for each country listed
            {
                Document doc    = Jsoup.connect("http://www.tradingeconomics.com/" + countries[countriesVar] + "/manufacturing-pmi").get();   // opens URL, dynamic based on countries array above        
                Elements links  = doc.select("a");      // pulls out all links (good practice)

                for (Element table : doc.select("table.table.table-condensed")) // selects the correct table from the HTML page
                {
                    for (Element row : table.select("tr"))      // loops through all rows in table
                    {
                        Elements tds = row.select("td");        // loops through all cells in that row of table

                        if(tds.size() == 9) // the lines that we want all have 10 elements
                        {
                            System.out.printf("%-15s %10s %5s %15s %5s %n", countries[countriesVar],"Current: ",tds.get(1).text(),"Previous: ",tds.get(2).text()); // formats output to console, prints 2nd and 3rd elements
                            fmtFile.format("%15s %15s %15s %n", countries[countriesVar],tds.get(1).text(),tds.get(2).text());   // formats output to txt file, prints 2nd annd 3rd elements
                        }

                    }

                }

            } // end of countries for loop
                fmtFile.close();

        } // end of main
        
} // end of public class PMI_Scraper