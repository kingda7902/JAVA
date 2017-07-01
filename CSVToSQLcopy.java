package jdbc.hw;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.*;
import com.opencsv.*;

/*
 * MissingMigrantsProject.csv to mySQL
 *
 CREATE TABLE `missingmigrantsproject` (
  `id` decimal(6,0) NOT NULL,
  `cause_of_death` varchar(200) DEFAULT NULL,
  `region_origin` varchar(50) DEFAULT NULL,
  `affected_nationality` varchar(200) DEFAULT NULL,
  `missing` int(11) zerofill DEFAULT NULL,
  `dead` int(11)  zerofill DEFAULT NULL,
  `incident_region` varchar(50) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `source` varchar(100) DEFAULT NULL,
  `reliability` varchar(50) DEFAULT NULL,
  `lat` double  DEFAULT NULL,
  `lon` double  DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
 * 
 * 
 * */

public class CSVToSQL {

	public static void main(String[] args)  {
		
		File mM = new File("D:\\Downloads\\missingmigrants\\MissingMigrantsProject.csv");
		FileReader fr = null;
		CSVReader csvr = null;
		String[] header = null;
		int colNumber = 0;
		
		
		String sqlUrl ="jdbc:mysql://localhost:3306/jdbc";
		Connection conn = null;
		
		try {
			fr = new FileReader(mM);
			csvr = new CSVReader(fr);
			conn = DriverManager.getConnection(sqlUrl,"user","pw");
			
			//col_name & length
			header = csvr.readNext();
			colNumber = header.length;
			
//			String[] tmpString;
//			while ( ( tmpString = csvr.readNext()) != null) {
//				for ( int i = 0; i < colNumber; i++) {
//					System.out.print(tmpString[i] + " ");
//				}
//				System.out.println();
//			}
			
			
			
			//set to sql
			
			StringBuilder qLong = new StringBuilder("?") ;
			for ( int i = 1; i < colNumber; i++) {
				qLong.append(",?");
			}
			String stmt = "INSERT INTO missingmigrantsproject VALUES(" + qLong.toString() + ");";
			PreparedStatement pstmt = conn.prepareStatement(stmt);
			
			String[] tmpString = null;
			int count = 0;
			while ( ( tmpString = csvr.readNext()) != null) {
					for ( int i = 0; i < colNumber; i++) {
					String setStr = tmpString[i];
					//"" to int
					if (setStr.equals("")) setStr = null;
					// date formatter
					if ( i == 7 && setStr != null) {
						SimpleDateFormat oldFormatter = new SimpleDateFormat("dd/MM/yyyy");
						SimpleDateFormat sqlFormatter = new SimpleDateFormat("yyyy/MM/dd");
				        try {
				            java.util.Date date = oldFormatter.parse(setStr);
				            setStr = sqlFormatter.format(date).toString();  
				        } catch (ParseException e) {
				            e.printStackTrace();
				        }
					}
					
					
					pstmt.setString((i+1),setStr);
					System.out.print(setStr + " #"+ (i+1) +"|");
					
				}
				System.out.println();
				pstmt.addBatch();
				if ( count++ > 100 ) {
					pstmt.executeBatch();
					count = 0;
				}
			}
			pstmt.executeBatch();

		} catch ( FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null) conn.close();
			} catch (SQLException e){
				
			}
			
			try {
				if (csvr != null) csvr.close();
			} catch (IOException e){
				
			}
			
			try {
				if (fr != null) fr.close();
			} catch (IOException e){
				
			}
		}
		
		
//		System.out.println("Column Number:" + colNumber);
//		for (int i = 0; i < colNumber ; i++){
//			System.out.println(header[i]);
//		}
//		System.out.println();
//		
		

	}
	

}
