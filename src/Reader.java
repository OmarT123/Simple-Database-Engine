import java.io.*;
import java.util.*;

public class Reader {
	

	// reads a CSV file and returns as a 2d array
	public static String[][] readCSV(String filePath) {
		List<String[]> res = new ArrayList<>();
		String[][] data = null;
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			String tuple;
			while ((tuple = reader.readLine()) != null) {
				String[] temp = tuple.split(",");
				res.add(temp);
			}
			int numRows = res.size();
			int numCols = res.get(0).length;
			data = new String[numRows][numCols];
			for (int i = 0; i < numRows; i++) {
				data[i] = res.get(i);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data;
	}
	
	// reads a CSV file and returns as a 2d array with 
		public static String[][] readNSizeTable(String filePath) {
			List<String[]> res = new ArrayList<>();
			String[][] data = null;
			try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
				String tuple;
				while ((tuple = reader.readLine()) != null) {
					String[] temp = tuple.split(",");
					res.add(temp);
				}
				int numRows = res.size();
				int numCols = res.get(0).length;
				
				//get 201 from metadata
				
				data = new String[201][numCols];
				for (int i = 0; i < numRows; i++) {
					data[i] = res.get(i);
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return data;
		}
	
	//returns data for a specific table
	public static String[][] readTableMeta(String[][] metaData, String tableName) {
		List<String[]> list = new ArrayList<>();
		for (int i = 1; i < metaData.length; i++) {
			if (metaData[i][0].equals(tableName)) {
				list.add(metaData[i]);
			}
		}
		int numRows = list.size();
		int numCols = list.get(0).length;
		String[][] data = new String[numRows][numCols];
		for (int i = 0; i < numRows; i++) {
			data[i] = list.get(i);
		}
		return data;
	}
}
