import java.io.IOException;
import java.lang.constant.Constable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class DBApp {
	public static Writer writer;
	public static Reader reader;
	public static Hashtable<String, Table> tables;

	public static void init() {
		writer = new Writer();
		reader = new Reader();
		tables = new Hashtable<>();
		createMetaDataHeader();
	}

	public static void createMetaDataHeader() {
		String csvFilePath = "metadata.csv";
		String headers = "TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType,min,max,ForeignKey,ForeignTableName,ForeignColumnName,Computed";
		writer.overwriteFile(csvFilePath, headers);
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax, Hashtable<String, String> htblForeignKeys, String[] computedCols)
			throws DBAppException {
		Table t = new Table(strTableName);
		String csvFilePath = "metadata.csv";
		ArrayList<String> arrayList = new ArrayList<>();
		Iterator<Map.Entry<String, String>> iterator = htblColNameType.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			StringBuilder row = new StringBuilder();
			row.append(strTableName);
			row.append(",");
			String colName = entry.getKey();
			row.append(colName);
			row.append(",");
			String colType = entry.getValue();
			row.append(colType);
			row.append(",");
			row.append(strClusteringKeyColumn.equals(colName) ? "true" : "false");
			row.append(",");
			row.append("null");
			row.append(",");
			row.append("null");
			row.append(",");
			String colMin = htblColNameMin.get(colName);
			row.append(colMin);
			row.append(",");
			String colMax = htblColNameMax.get(colName);
			row.append(colMax);
			row.append(",");
			String foreign;
			String[] foreignTableCol;
			String tableName = null;
			String tableCol = null;
			if (htblForeignKeys.containsKey(colName)) {
				foreign = htblForeignKeys.get(colName);
				foreignTableCol = foreign.split("\\.");
				tableName = foreignTableCol[0];
				tableCol = foreignTableCol[1];
			}
			row.append(tableName == null ? "false" : "true");
			row.append(",");
			row.append(tableName);
			row.append(",");
			row.append(tableCol);
			row.append(",");
			boolean computed = listContains(colName, computedCols);
			row.append(computed ? "true" : "false");
			arrayList.add(row.toString());
		}
		for (int i = 0; i < arrayList.size(); i++) {
			writer.appendToFile(csvFilePath, arrayList.get(i));
		}
		tables.put(strTableName, t);
	}

	public static boolean listContains(String s, String[] arr) {
		for (int i = 0; i < arr.length; i++) {
			if (s.equals(arr[i]))
				return true;
		}
		return false;
	}

	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		if (strarrColName.length != 2)
			throw new DBAppException("Two columns required to create an index");

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		Table curTable = tables.get(strTableName);

		// Get tuple to be inserted in Table
		String tuple = getTuple(strTableName, htblColNameValue);

		if (curTable.hasIndex()) {
			// if an index is created on the table
		} else {
			// No index is created on the table
			// so insert according to clustering key
			if (curTable.getPages().isEmpty()) {
				// Table is currently empty
				// create a new page and store the first tuple in it
				String filePath = curTable.getPath() + "Page" + (curTable.getPages().size() + 1) + ".csv";
				System.out.println(filePath);
				File file = new File(filePath);
				try {
					if (file.createNewFile()) {
						System.out.println("File created successfully.");
					} else {
						System.out.println("File already exists.");
					}
				} catch (IOException e) {
					System.out.println("An error occurred while creating the file: " + e.getMessage());
				}
				curTable.getPages().add(file);
				
				//store names of cols
				
				writer.appendToFile(filePath, tuple);
			} else {
				// Table already has Pages
			}
		}
	}

	// verify input data with metadata file
	// check min, max, foreign key, computed (if computed value is inserted throw an
	// error)
	// create tuple to be inserted
	public static String getTuple(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
//		for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
//            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue().toString());
//        }
		StringBuilder sb = new StringBuilder();
		String[][] metaData;
		try {
			metaData = reader.readCSV("metadata.csv");
			String[][] tableMeta = reader.readTableMeta(metaData, strTableName);
			for (int i = 0; i < tableMeta.length; i++) {
				// getting column name and its value
				String colName = tableMeta[i][1];
				boolean computed = tableMeta[i][11].equals("true")? true : false;
				if (!htblColNameValue.containsKey(colName) && computed)
				{
					//compute value
					
					continue;
				}
				if (htblColNameValue.containsKey(colName) && computed)
					throw new DBAppException(colName + " is a computed col and should not be inserted");
				Object value = htblColNameValue.get(colName);
				System.out.println(value.toString());
				// getting control data from csv file to verify the input tuple
				String colType = tableMeta[i][2];
				String min = tableMeta[i][6];
				String max = tableMeta[i][7];
				
				//check for foreign key
				
				Class colClass = Class.forName(colType);
				Constructor constructor = colClass.getConstructor(String.class);
				Object minObj = constructor.newInstance(min);
				Object maxObj = constructor.newInstance(max);

				// comparing control data with input tuple
				if (((Comparable) value).compareTo(minObj) < 0)
					throw new DBAppException(colName + " value is less than the minimum value (" + min + ")");
				if (((Comparable) value).compareTo(maxObj) > 0)
					throw new DBAppException(colName + " value is greater than the maximum value (" + max + ")");
				sb.append(value.toString());
				if (i != tableMeta.length - 1)
					sb.append(",");
				System.out.println("here");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		return null;
	}

}
