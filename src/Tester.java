import java.io.File;
import java.io.IOException;
import java.util.Hashtable;

public class Tester {

	public static void printGrid(String[][] grid) {
		for (String[] row : grid) {
			for (String element : row) {
				System.out.print(element + " ");
			}
			System.out.println();
		}
	}

	public static void main(String[] args) {

		try {
			DBApp db = new DBApp();
			db.init();
			Hashtable<String, String> htblColNameType = new Hashtable<>();
			htblColNameType.put("ProductID", "java.lang.Integer");
			htblColNameType.put("ProductName", "java.lang.String");
			htblColNameType.put("ProductPrice", "java.lang.Double");
			Hashtable<String, String> htblColNameMin = new Hashtable<>();
			htblColNameMin.put("ProductID", "0");
			htblColNameMin.put("ProductName", "A");
			htblColNameMin.put("ProductPrice", "0");
			Hashtable<String, String> htblColNameMax = new Hashtable<>();
			htblColNameMax.put("ProductID", "1000");
			htblColNameMax.put("ProductName", "ZZZZZZZZ");
			htblColNameMax.put("ProductPrice", "100000");
			Hashtable<String, String> htblForeignKeys = new Hashtable<>();
			String[] computedCols = new String[1];
			db.createTable("Product", "ProductID", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys,
					computedCols);
			Hashtable<String, Object> htblColNameVal = new Hashtable<>();
			for (int i = 0; i <= 200; i++) {
				htblColNameVal = new Hashtable<>();
				htblColNameVal.put("ProductID", new Integer(i));
				htblColNameVal.put("ProductName", new String("Power Bank"));
				htblColNameVal.put("ProductPrice", new Double(15.5));
				db.insertIntoTable("Product", htblColNameVal);
			}
			htblColNameVal = new Hashtable<>();
			htblColNameVal.put("ProductName", new String("Updated"));
			db.updateTable("Product", "1", htblColNameVal);
			htblColNameVal = new Hashtable<>();
			htblColNameVal.put("ProductID", new Integer(1));
			htblColNameVal.put("ProductName", new String("Power Bank"));
			htblColNameVal.put("ProductPrice", new Double(15.5));
			db.deleteFromTable("Product",htblColNameVal);
//			GridIndex index = new GridIndex("Index", "Table", "Employee", "0", "1000", "Worker", "0", "1000");
//			index.printIndex();
			System.out.println("Terminated");
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		

		
	}
}
