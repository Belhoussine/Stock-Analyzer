import java.util.Properties;
import java.util.Deque;
import java.util.LinkedList;
import java.util.ArrayList;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class Main {
	static Connection readerConn = null;
	static Connection writerConn = null;
	static double divisor;
	static Deque<StockDay> companyStocks = new LinkedList<StockDay>();
	static ArrayList<ArrayList<Output>> intervals = new ArrayList<>();
	static ArrayList<String> industry = new ArrayList<String>();
	static ArrayList<Integer> tickerCount = new ArrayList<Integer>();
	static ArrayList<Double> totalReturn = new ArrayList<Double>();
	static String[] sentinel={" "," ","0","0","0","0","0","0"};
	static boolean first;
	static ArrayList<String> startDates = new ArrayList<String>();
	static ArrayList<String> endDates = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
		
		final String dropPerformanceTable = "drop table if exists Performance; ";
	    final String createPerformanceTable = "create table Performance (Industry char(30), Ticker char(6), StartDate char(10), EndDate char(10), TickerReturn char(12), IndustryReturn char(12));";
	    
	    //Initializing variables
	    divisor=1;
	    companyStocks.clear();
	    industry.clear();
	    tickerCount.clear();
	  
        String readParamsFile = "readerparams.txt";
        String writeParamsFile = "writerparams.txt";
        
        if (args.length >= 2) {
            readParamsFile = args[0];
            writeParamsFile = args[1];
        }
        
        // Get connection properties for writing and reading
        Properties readprops = new Properties(), writeprops = new Properties();
        readprops.load(new FileInputStream(readParamsFile));
        writeprops.load(new FileInputStream(writeParamsFile));
        
        try {
            // Get Reader Database connection
            Class.forName("com.mysql.jdbc.Driver");
            String readdburl = readprops.getProperty("dburl");
            readerConn = DriverManager.getConnection(readdburl, readprops);
            System.out.printf("Reader connection established\n");
            
            // Get Writer Database connection
            Class.forName("com.mysql.jdbc.Driver");
            String writedburl = writeprops.getProperty("dburl");
            writerConn = DriverManager.getConnection(writedburl, writeprops);
            System.out.printf("Writer connection established\n");
            Statement stmt = writerConn.createStatement();
            
            //Creating Performance table
            stmt.execute(dropPerformanceTable);
    	    stmt.execute(createPerformanceTable );
    	    
    	    // Getting and processing the industries
            getIndustries();
            
            //Closing Database connections
            readerConn.close();
            writerConn.close();
            System.out.printf("\nDatabase connections closed.\n");
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }
	
    //Functions
	
	// Get the industries
	static void getIndustries() throws SQLException {
		PreparedStatement stmt = readerConn.prepareStatement("select Industry, count(distinct Ticker) as TickerCnt from Company natural join PriceVolume group by Industry order by TickerCnt DESC, Industry;");
		ResultSet industries = stmt.executeQuery();
		int cnt=0;
		
		//Getting the different industries in database and storing them
		while(industries.next()) {
			tickerCount.add(cnt,industries.getInt("TickerCnt"));
			industry.add(cnt++,industries.getString("Industry"));	
		}
		System.out.printf("%d industries found\n", cnt);
		
		//Printing Industries Names
		for(int i=0; i < cnt; i++) {
			System.out.printf("%s \n", industry.get(i));
		}
		
		//Processing each company in the industry 
		for(int i=0; i < cnt; i++) {
			System.out.printf("\nProcessing %s \n", industry.get(i));
			getCompanies(industry.get(i),tickerCount.get(i));
		}
	}
	
	//Get the companies for each industry
	static void getCompanies(String ind, int tc) throws SQLException {  
		
	    //Initializing these values for each distinct industry
	    String startDate="", endDate="";
		int minTrade=(int)1e9;
		first = true;
		startDates.clear();
		endDates.clear();
		intervals.clear();
		totalReturn.clear();
		
		// Getting the maximum TransDate from each company's minimum TransDate
		PreparedStatement stmt1 = readerConn.prepareStatement("select transdate from Company natural join PriceVolume where Industry =? and transdate >= all(select min(TransDate) mi from Company natural join PriceVolume where Industry =? group by Ticker having count(distinct Transdate) >=150 order by Ticker)order by transdate limit 1");
		stmt1.setString(1, ind); 
		stmt1.setString(2, ind); 
		ResultSet minmax = stmt1.executeQuery();
		
		// Getting the minimum TransDate from each company's maximum TransDate
		PreparedStatement stmt2 = readerConn.prepareStatement("select transdate from Company natural join PriceVolume where Industry =? and transdate <= all(select max(TransDate) ma from Company natural join PriceVolume where Industry =? group by Ticker having count(distinct Transdate) >=150 order by Ticker) order by transdate desc limit 1");
		stmt2.setString(1, ind);
		stmt2.setString(2, ind); 
		ResultSet maxmin = stmt2.executeQuery();
		
		// Storing those dates
		if (minmax.next() && maxmin.next()) {
			startDate = minmax.getString("transdate");
			endDate = maxmin.getString("transdate");
		}
		
		// Querying company data based on previously stored dates
		PreparedStatement stmt3 = readerConn.prepareStatement("select Ticker, min(TransDate) mi, max(TransDate) ma,count(distinct TransDate) as TradingDays from Company natural join PriceVolume where Industry = ? and TransDate >= ? and TransDate <= ? group by Ticker having TradingDays >= 150 order by Ticker;");
		stmt3.setString(1, ind);
		stmt3.setString(2, startDate); 
		stmt3.setString(3, endDate);
		ResultSet companies = stmt3.executeQuery();
		
		while(companies.next()) {
            divisor=1;
            // Initializing company array for each iteration
            companyStocks.clear();
            
            //Extracting data from received from the query
			StockDay nextDay=new StockDay(sentinel);
			String company=companies.getString("Ticker");
			String minTransDate=companies.getString("mi");
			String maxTransDate=companies.getString("ma");
			int tradingDays=companies.getInt("TradingDays");
			
			// Getting minimum number of trading days (since it can vary)
			if(tradingDays < minTrade)
					minTrade=tradingDays;
			
			// Querying data for given time interval
			queryData(company, minTransDate, maxTransDate, nextDay);
			
			//If the company being processed is the first one, use it to get the intervals
			if(first)
				getIntervals((int)tradingDays/60);
			fillData(ind, company, endDates.size());
			first=false;
		} 
		
		//If minTrade did not change, then there was no trading day, else display info
		if(minTrade == (int)1e9)
			System.out.printf("Insufficient data for Information Technology => no analysis\n");
		else {
			System.out.printf("%d accepted tickers for %s(%s - %s), %d common dates\n", tc, ind, startDate, endDate, minTrade);
			
			// Preparing the writer statement
			PreparedStatement prepstmt = writerConn.prepareStatement("insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn) values(?, ?, ?, ?, ?, ?);");
			
			//Initializing loop variables
			int columns=intervals.get(0).size();
			int rows=intervals.size();
			
			//Filling the industryReturn using the pre-computed sum stored in totalReturn and subtracting tickerReturn 
			//of current company then dividing by (tickerCount - 1). Also  filling the Performance table along the way
			for(int j= 0; j < columns; j++) {
				for(int i = 0; i < rows; i++) {
					intervals.get(i).get(j).IndustryReturn = (totalReturn.get(j) - intervals.get(i).get(j).TickerReturn) / (tc-1);
					prepstmt.setString(1, intervals.get(i).get(j).industry);
					prepstmt.setString(2, intervals.get(i).get(j).ticker);
					prepstmt.setString(3, intervals.get(i).get(j).StartDate);
					prepstmt.setString(4, intervals.get(i).get(j).EndDate);
					prepstmt.setString(5,  String.format("%10.7f", intervals.get(i).get(j).TickerReturn));
					prepstmt.setString(6,  String.format("%10.7f", intervals.get(i).get(j).IndustryReturn));
					prepstmt.executeUpdate();
				}
			}
		}
	}
	
	//Filling output Data except industryReturn which is filled in function getCompanies
	static void fillData(String ind, String company, int numIntervals) {
		
		int i;
		StockDay current;
		ArrayList <Output> companyData = new ArrayList<Output>();
		
		for(i = 1; i < numIntervals ; i++) {
			
			String sd= startDates.get(i);
			// Getting first day in the interval and extracting the opening price
			current=companyStocks.pop();
			double open=current.op;
			
			//Creating Output object
			Output out = new Output(ind, company, current.date);
			
			// Advancing until reaching the day before the first day in next interval
			while(companyStocks.peek().date.compareTo(sd) < 0) {
				current=companyStocks.pop();
			}
			// Extracting closing price from (already popped) last day in interval
			double close=current.cp;
			
			// Filling output object's attributes with extracted data
			out.EndDate=current.date;
			out.TickerReturn=(close / open)-1;
			
			//Filling totalReturn array: sum of all companies' ticker returns in given interval (Used to compute Industry return more efficiently)
			if(first) {
				totalReturn.add((close / open)-1);
			}else {
				totalReturn.set(i-1,totalReturn.get(i-1)+(close / open) -1);
			}
			// Adding filled output object to array of output objects
			companyData.add(out);
		}
		String ed= endDates.get(i-1);
		// Getting first day in the interval and extracting the opening price
		current=companyStocks.pop();
		double open=current.op;
		Output out = new Output(ind, company, current.date);
		// Advancing until reaching the last interval day
		while(companyStocks.peek().date.compareTo(ed) <= 0) {
			current=companyStocks.pop();
		}
		double close=current.cp;
		// Filling output object's attributes with extracted data
		out.EndDate=current.date;
		out.TickerReturn=(close / open)-1;
		//Filling totalReturn array: sum of all companies' ticker returns in given interval (Used to compute Industry return more efficiently)
		if(first) {
			totalReturn.add((close / open)-1);
		}else {
			totalReturn.set(i-1,totalReturn.get(i-1)+(close / open) -1);
		}
		// Adding filled output object to array of output objects
		companyData.add(out);
		// Adding companyData to the 2 dimension array intervals
		intervals.add(companyData);
	}
	
    // Setting the intervals using the first company in the industry group
	static void getIntervals(int intervalNum) {
    	int cnt=1;
    	LinkedList<StockDay> compStocks = (LinkedList<StockDay>) companyStocks;
    	// Extracting starting dates and ending dates of the intervals
    	for(int i=0; i < companyStocks.size(); i++) {
    		if(cnt % 60 == 1) {
	    		startDates.add(compStocks.get(i).date);
	    	}
    		else if(cnt % 60 == 0) {
	    		endDates.add(compStocks.get(i).date);
	    	}
	    	cnt++;
    	}
	}
	
    // Create and execute a query based on given interval
    static void queryData(String ticker, String StartDate, String EndDate, StockDay nextDay) throws SQLException {
        
    	PreparedStatement stmt1 = readerConn.prepareStatement("select * from PriceVolume where Ticker = ? and TransDate between ? and ? order by TransDate DESC");
    	stmt1.setString(1, ticker);
    	stmt1.setString(2, StartDate);
    	stmt1.setString(3, EndDate);
    	ResultSet priceVolumes = stmt1.executeQuery();
    	applySplits(priceVolumes, nextDay);
    	stmt1.close();

    }
       
    
    // Apply Splits to given Company
    static void applySplits(ResultSet priceVolumes, StockDay nextDay) throws SQLException { 

        while (priceVolumes.next()) {
        	String[] values = {priceVolumes.getString("Ticker"),priceVolumes.getString("TransDate"),priceVolumes.getString("OpenPrice"),priceVolumes.getString("HighPrice"),priceVolumes.getString("LowPrice"),priceVolumes.getString("ClosePrice"),priceVolumes.getString("Volume"),priceVolumes.getString("AdjustedClose")};
        	StockDay sd= new StockDay(values);
        	sd.op/=divisor;sd.cp/=divisor;sd.hp/=divisor;sd.lp/=divisor;  
        	switch(isSplit(sd,nextDay)){
        		case 0: break;
            	case 1: 
            			sd.op/=2.0;sd.cp/=2.0;sd.hp/=2.0;sd.lp/=2.0;  
    					divisor*=2.0;
    					break;
            	case 2: 
            			sd.op/=3.0;sd.cp/=3.0;sd.hp/=3.0;sd.lp/=3.0;  
            			divisor*=3.0;
            			break;
            	case 3: 
            			sd.op/=1.5;sd.cp/=1.5;sd.hp/=1.5;sd.lp/=1.5;  
            			divisor*=1.5;
                    	break;
        	}	
        	nextDay= sd;
        	companyStocks.push(sd);
        }
    }
    
    //Detecting if it is a split
    static int isSplit(StockDay firstDay, StockDay nextDay){
        if(nextDay.op == 0)
          return 0;
        if (Math.abs((firstDay.cp/nextDay.op)-2.0)<0.20)
          return 1;
        if(Math.abs((firstDay.cp/nextDay.op)-3.0)<0.30)
          return 2;
        if(Math.abs((firstDay.cp/nextDay.op)-1.5)<0.15)
          return 3;
        return 0;
    }

}

