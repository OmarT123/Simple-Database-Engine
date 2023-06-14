import java.io.File;
import java.util.*;
import java.io.*;
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
			for (int i = 0; i <= 210; i++) {
				htblColNameVal = new Hashtable<>();
				htblColNameVal.put("ProductID", new Integer(i));
				htblColNameVal.put("ProductName", new String("Power Bank"));
				htblColNameVal.put("ProductPrice", new Double(15.5));
				db.insertIntoTable("Product", htblColNameVal);
			}

			htblColNameType = new Hashtable<>();
			htblColNameType.put("SaleID", "java.lang.Integer");
			htblColNameType.put("SaleDate", "java.lang.Date");
			htblColNameType.put("ProductID", "java.lang.Integer");
			htblColNameType.put("Quantity", "java.lang.Integer");
			htblColNameType.put("TotalAmount", "java.lang.Double");
			htblColNameMin = new Hashtable<>();
			htblColNameMin.put("SaleID", "0");
			htblColNameMin.put("SaleDate", "01.01.2000");
			htblColNameMin.put("ProductID", "0");
			htblColNameMin.put("Quantity", "1");
			htblColNameMin.put("TotalAmount", "0");
			htblColNameMax = new Hashtable<>();
			htblColNameMax.put("SaleID", "10000");
			htblColNameMax.put("SaleDate", "31.12.2030");
			htblColNameMax.put("ProductID", "1000");
			htblColNameMax.put("Quantity", "10000");
			htblColNameMax.put("TotalAmount", "1000000000");
			htblForeignKeys.put("ProductID", "Product.ProductID");
			String[] computed = new String[1];
			db.createTable("Sale", "SaleID", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys,
					computed);

			for (int i = 0; i < 100; i++) {
				htblColNameVal = new Hashtable<>();
				htblColNameVal.put("SaleID", new Integer(i));
				htblColNameVal.put("SaleDate", new Date(2023, 21, 3));
				htblColNameVal.put("ProductID", new Integer(i));
				htblColNameVal.put("Quantity", new Integer(i + 1));
				htblColNameVal.put("TotalAmount", new Double(i + 200));
				db.insertIntoTable("Sale", htblColNameVal);
			}

//			htblColNameVal = new Hashtable<>();
//			htblColNameVal.put("ProductID", new Integer(1));
//			db.deleteFromTable("Product",htblColNameVal);
//			SQLTerm[] arrsqlSqlTerms = new SQLTerm[2];
//			arrsqlSqlTerms[0] = new SQLTerm("Sale", "TotalAmount", ">", new Double(250));
//			arrsqlSqlTerms[1] = new SQLTerm("Sale", "Quantity", ">", new Integer(3));
//			String[] operators = new String[1];
//			operators[0] = "AND";
//			Iterator resultSet = db.selectFromTable(arrsqlSqlTerms, operators);
//			while (resultSet.hasNext()) {
//				System.out.println(db.convertToString((String[]) resultSet.next()));
//			}

			String[] ind = { "ProductID", "ProductPrice" };
			db.createIndex("Product", ind);

			htblColNameVal = new Hashtable<>();
			htblColNameVal.put("ProductID", new Integer(800));
			htblColNameVal.put("ProductName", new String("Power Bank"));
			htblColNameVal.put("ProductPrice", new Double(50000));
			db.insertIntoTable("Product", htblColNameVal);
			htblColNameVal = new Hashtable<>();
			htblColNameVal.put("ProductID", new Integer(801));
			htblColNameVal.put("ProductName", new String("Power Bank"));
			htblColNameVal.put("ProductPrice", new Double(51001));
			db.insertIntoTable("Product", htblColNameVal);
			
			System.out.println("Terminated");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
