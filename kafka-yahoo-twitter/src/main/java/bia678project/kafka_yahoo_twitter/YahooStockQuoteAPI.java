package bia678project.kafka_yahoo_twitter;

import java.io.IOException;
import java.util.Map;

import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

public class YahooStockQuoteAPI {
	public static void main(String[] args) throws IOException {
		String[] symbols = new String[] {"INTC", "BABA", "TSLA", "AIR.PA", "GOOG","MSFT"};
		Map<String, Stock> stocks = YahooFinance.get(symbols);
		
		Stock stock1 = stocks.get("INTC");
		StockQuote sq = stock1.getQuote();
		System.out.println("-----INTC-----");
	    System.out.println("Symbol: " + sq.getSymbol());
	    System.out.println("Price: " + sq.getPrice());
	    System.out.println("Date: " + sq.getLastTradeTime().getTime());
	    
		Stock stock2 = stocks.get("BABA");
		StockQuote sq1 = stock2.getQuote();
		System.out.println("-----INTC-----");
	    System.out.println("Symbol: " + sq1.getSymbol());
	    System.out.println("Price: " + sq1.getPrice());
	    System.out.println("Date: " + sq1.getLastTradeTime().getTime());
	    
		Stock stock3 = stocks.get("BABA");
		StockQuote sq2 = stock3.getQuote();
		System.out.println("-----BABA-----");
	    System.out.println("Symbol: " + sq2.getSymbol());
	    System.out.println("Price: " + sq2.getPrice());
	    System.out.println("Date: " + sq2.getLastTradeTime().getTime());
	    
	    
		Stock stock = stocks.get("GOOG");
		StockQuote sq3 = stock.getQuote();
		System.out.println("-----GOOG-----");
	    System.out.println("Symbol: " + sq3.getSymbol());
	    System.out.println("Price: " + sq3.getPrice());
	    System.out.println("Date: " + sq3.getLastTradeTime().getTime());
		
	    
		Stock stock4 = stocks.get("MSFT");
		StockQuote msft = stock4.getQuote();
		System.out.println("-----MSFT-----");
	    System.out.println("Symbol: " + msft.getSymbol());
	    System.out.println("Price: " + msft.getPrice());
	    System.out.println("Date: " + msft.getLastTradeTime().getTime());
	}
}
