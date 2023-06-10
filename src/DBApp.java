import java.io.IOException;
import java.lang.constant.Constable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.text.DateFormat;

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
		Table t = new Table(strTableName, strClusteringKeyColumn);
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
			row.append(strClusteringKeyColumn.equals(colName) ? "True" : "False");
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
			row.append(tableName == null ? "False" : "True");
			row.append(",");
			row.append(tableName);
			row.append(",");
			row.append(tableCol);
			row.append(",");
			boolean computed = listContains(colName, computedCols);
			row.append(computed ? "True" : "False");
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
		try {
			Table curTable = tables.get(strTableName);
			// Get tuple to be inserted in Table and verify it with meta data
			String[] tupleInfo = getTuple(strTableName, htblColNameValue);
			String tuple = tupleInfo[0];
			String clusteringCol = tupleInfo[1];
			String clusteringType = tupleInfo[2];
			String clusteringVal = tupleInfo[3];
			Class colClass = Class.forName(clusteringType);
			Constructor constructor = colClass.getConstructor(String.class);
			Object clusterVal = constructor.newInstance(clusteringVal);
			if (curTable.hasIndex()) {
				// if an index is created on the table
			} else {
				// No index is created on the table
				// so insert according to clustering key
				if (curTable.getPages().isEmpty()) {
					// Table is currently empty
					// create a new page and store the first tuple in it
					String filePath = curTable.getPath() + "Page" + (curTable.getPages().size() + 1) + ".csv";
					createPage(curTable, filePath);
					writer.appendToFile(filePath, tuple);
				} else {
					// Table already has Pages
					// iterate over existing pages/tuples to find location of current tuple
					// shift any other tuples that are below the current tuple
					int rowReq = -1;
					int pageReq = -1;
					int row = 1;
					File firstPage = curTable.getPages().get(0);
					String filePath = firstPage.getPath();
					String[][] pageData = reader.readNSizeTable(filePath);
					int clusterColIndex = 0;
					String[] header = pageData[0];
					for (int j = 0; j < header.length; j++) {
						if (header[j].equals(clusteringCol)) {
							clusterColIndex = j;
							break;
						}
					}
					for (int i = 0; i < curTable.getPages().size(); i++) {
						File curPage = curTable.getPages().get(i);
						pageData = reader.readNSizeTable(curPage.getPath());
						for (row = 1; (row < pageData.length) && pageData[row][clusterColIndex] != null; row++) {
							// prepare the values to be compared
							// compare the values to find index of insertion
							Object curVal = constructor.newInstance(pageData[row][clusterColIndex]);
							if (((Comparable) clusterVal).compareTo(curVal) < 0) {
								rowReq = row;
								pageReq = i;
								break;
							} else if (((Comparable) clusterVal).compareTo(curVal) == 0)
								throw new DBAppException("Can not have duplicates of clustering key value");
						}
					}
					if (rowReq == -1) {
						rowReq = row;
						pageReq = curTable.getPages().size() - 1;
					}
					// shift tuples if required
					String[][] page = reader.readNSizeTable(curTable.getPages().get(pageReq).getPath());
					if (rowReq >= 201) {
						filePath = curTable.getPath() + "Page" + (curTable.getPages().size() + 1) + ".csv";
						createPage(curTable, filePath);
						writer.appendToFile(filePath, tuple);
					} else if (page[rowReq][0] != null) {
						page = shiftTuples(curTable, pageReq, rowReq);
						page[row] = tuple.split(",");
						filePath = curTable.getPages().get(pageReq).getPath();
						writer.writePage(filePath, page);
					} else {
						page[rowReq] = tuple.split(",");
						filePath = curTable.getPages().get(pageReq).getPath();
						writer.writePage(filePath, page);
					}
				}
			}
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| ClassNotFoundException | NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// shift tuples in the table for insertion method
	public static String[][] shiftTuples(Table table, int page, int row) {
		int i = table.getPages().size() - 1;
		int j = -1;
		for (; i >= page; i--) {
			String[][] pageContent = reader.readNSizeTable(table.getPages().get(i).getPath());
			for (j = pageContent.length - 1; j >= 1 && j != row; j--) {
				while (pageContent[j][0] == null) {
					j--;
				}
				if (j == 1 && i != table.getPages().size() - 1) {
					// if at the beginning of a file and need to read from previous file
					String[][] prevPage = reader.readNSizeTable(table.getPages().get(i - 1).getPath());
					String filePath = table.getPath() + "Page" + (table.getPages().size() + 1) + ".csv";

					// get 201 from metadata

					pageContent[1] = prevPage[200];
					writer.writePage(filePath, pageContent);
				} else if (j == 200 && i == table.getPages().size() - 1) {
					// if at the last tuple of the last page
					// need to create a new page and shift into it
					String filePath = table.getPath() + "Page" + (table.getPages().size() + 1) + ".csv";
					createPage(table, filePath);
					writer.appendToFile(filePath, convertToString(pageContent[j]));
				} else if (j < 200) {
					// regular case
					// normal shifting
					while (j >= row) {
						pageContent[j + 1] = pageContent[j];
						j--;
					}
					return pageContent;
				}
			}
		}
		return null;
	}

	// create a new page for the table
	public static void createPage(Table table, String filePath) {
		File file = new File(filePath);
		try {
			if (file.createNewFile()) {
				// System.out.println("File created successfully.");
			} else {
				// System.out.println("File already exists.");
			}
		} catch (IOException e) {
			System.out.println("An error occurred while creating the file: " + e.getMessage());
		}
		table.getPages().add(file);
		StringBuilder colHeaders = new StringBuilder();
		String[][] metaData = reader.readCSV("metadata.csv");
		String[][] tableMeta = reader.readTableMeta(metaData, table.getName());
		for (int i = 0; i < tableMeta.length; i++) {
			colHeaders.append(tableMeta[i][1]);
			if (tableMeta.length - 1 != i)
				colHeaders.append(",");
		}
		writer.overwriteFile(filePath, colHeaders.toString());

	}

	// verify input data with metadata file
	// check min, max, foreign key, computed (if computed value is inserted throw an
	// error)
	// create tuple to be inserted
	public static String[] getTuple(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		String[] res = new String[4];
		StringBuilder sb = new StringBuilder();
		String[][] metaData;
		try {
			metaData = reader.readCSV("metadata.csv");
			String[][] tableMeta = reader.readTableMeta(metaData, strTableName);
			for (int i = 0; i < tableMeta.length; i++) {
				if (tableMeta[i][3].equals("True")) {
					res[1] = tableMeta[i][1];
					res[2] = tableMeta[i][2];
				}
			}
			for (int i = 0; i < tableMeta.length; i++) {
				// getting column name and its value
				String colName = tableMeta[i][1];
				boolean computed = tableMeta[i][11].equals("true") ? true : false;
				if (!htblColNameValue.containsKey(colName) && computed) {
					// compute value

					continue;
				}
				if (htblColNameValue.containsKey(colName) && computed)
					throw new DBAppException(colName + " is a computed col and should not be inserted");
				Object value = htblColNameValue.get(colName);
				if (colName.equals(res[1]))
					res[3] = value.toString();
				// getting control data from csv file to verify the input tuple
				String colType = tableMeta[i][2];
				String min = tableMeta[i][6];
				String max = tableMeta[i][7];
				String foreign = tableMeta[i][8];
				// check for foreign key and data type
				if (foreign.equals("True")) {
					String foreignTable = tableMeta[i][9];
					String foreignCol = tableMeta[i][10];
					if (!checkForForeignKey(foreignTable, foreignCol, value.toString()))
						throw new DBAppException(
								value.toString() + " Foreign Key does not exist in table" + foreignTable);
				}
				Class colClass;
				Constructor constructor;
				Object minObj;
				Object maxObj;
				if (colType.equals("java.lang.Date")) {
					colClass = Date.class;
					int[] minDate = getDate(min);
					int[] maxDate = getDate(max);
					minObj = new Date(minDate[2], minDate[1], minDate[0]);
					maxObj = new Date(maxDate[2], maxDate[1], maxDate[0]);
				} else {
					colClass = Class.forName(colType);
					constructor = colClass.getConstructor(String.class);
					minObj = constructor.newInstance(min);
					maxObj = constructor.newInstance(max);
				}
				if (!colClass.equals(value.getClass()))
					throw new DBAppException(colName + " should be of type " + colType);
				// comparing control data with input tuple
				if (((Comparable) value).compareTo(minObj) < 0)
					throw new DBAppException(colName + " value is less than the minimum value (" + min + ")");
				if (((Comparable) value).compareTo(maxObj) > 0)
					throw new DBAppException(colName + " value is greater than the maximum value (" + max + ")");
				if (colType.equals("java.lang.Date"))
					value = new String(convertDateFormat(value.toString()));
				sb.append(value.toString());
				if (i != tableMeta.length - 1)
					sb.append(",");
			}
			res[0] = sb.toString();
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
		return res;
	}

	// changes date format from default to DD.MM.YYYY
	public static String convertDateFormat(String dateString) {
		DateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		DateFormat outputFormat = new SimpleDateFormat("dd.MM.yyyy");
		try {
			Date date = inputFormat.parse(dateString);
			String res = outputFormat.format(date);
			String[] arr = res.split("\\.");
			int year = Integer.parseInt(arr[2]) - 1900;
			int month = Integer.parseInt(arr[1]) - 1;
			arr[2] = "" + year;
			arr[1] = "" + month;
			StringBuilder sb = new StringBuilder();
			sb.append(arr[0]);
			sb.append(".");
			sb.append(arr[1]);
			sb.append(".");
			sb.append(arr[2]);
			return sb.toString();
		} catch (ParseException e) {
			e.printStackTrace();
			return null; // or throw an exception, depending on your use case
		}
	}

	// changes date format from DD.MM.YYYY to int[DD, MM, YYYY]
	public static int[] getDate(String s) {
		String[] date = s.split("\\.");
		int[] res = new int[3];
		res[0] = Integer.parseInt(date[0]);
		res[1] = Integer.parseInt(date[1]);
		res[2] = Integer.parseInt(date[2]);
		return res;
	}

	// checks if value is present in column colName in the table
	public static boolean checkForForeignKey(String table, String colName, String value) {
		Table t = tables.get(table);
		if (t.hasIndex()) {
			return false;
		}
		// if no index is created
		// loop on all pages and compare the value with the tuples
		for (int i = 0; i < t.getPages().size(); i++) {
			String[][] page = reader.readCSV(t.getPages().get(i).getPath());
			String[] header = page[0];
			int col;
			for (col = 0; col < header.length; col++) {
				if (header[col].equals(colName)) {
					break;
				}
			}
			for (int j = 1; j < page.length; j++) {
				if (page[j][col].equals(value)) {
					return true;
				}
			}
		}
		return false;

	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// search for the tuple using its clustering value
		// update the values, then write the tuple back in the table
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		try {
			Table curTable = tables.get(strTableName);
			if (curTable.hasIndex()) {
				// if there is an index created on the table required
			} else {
				// if no index is created on the table
				// search linearly through the entire table to find the required tuple
				String[][] headerAndTuple = findTuple(curTable, strClusteringKeyValue);
				String[] header = headerAndTuple[0];
				String[] tuple = headerAndTuple[1];

				// find changed values and change them in the tuple
				for (int i = 0; i < header.length; i++) {
					if (htblColNameValue.containsKey(header[i])) {
						String oldVal = tuple[i];
						// change its value in the tuple
						tuple[i] = htblColNameValue.get(header[i]).toString();
						// check if it is a foreign key in another table
						updateForeign(header[i], tuple[i], oldVal);
					}
				}
				// write the updated tuple back to the table
				writeUpdatedTuple(curTable, tuple);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void updateForeign(String colName, String newVal, String oldVal) {
		String[][] metaData = reader.readCSV("metadata.csv");
		for (int i = 1; i < metaData.length; i++) {
			if (metaData[i][1].equals(colName) && metaData[i][8].equals("True")) {
				String tableName = metaData[i][0];
				Table table = tables.get(tableName);
				for (int j = 0; j < table.getPages().size(); j++) {
					String[][] page = reader.readCSV(table.getPages().get(j).getPath());
					int col = 0;
					for (int k = 0; k < page.length; k++) {
						if (k == 0) {
							String[] header = page[0];
							for (col = 0; col < header.length; col++) {
								if (header[col].equals(colName))
									break;
							}
						} else if (page[k][col].equals(oldVal)) {
							page[k][col] = newVal;
							writeUpdatedTuple(table, page[k]);
						}
					}
				}
			}
		}
	}

	// searches for the location of the old tuple
	// and replaces it with the new one
	public static void writeUpdatedTuple(Table table, String[] tuple) {
		for (int i = 0; i < table.getPages().size(); i++) {
			String[][] page = reader.readCSV(table.getPages().get(i).getPath());
			int clusterColIndex = 0;
			for (int h = 0; h < page[0].length; h++) {
				if (table.getClusterCol().equals(page[0][h])) {
					clusterColIndex = h;
				}
			}
			for (int j = 1; j < page.length; j++) {
				if (page[j][clusterColIndex].equals(tuple[clusterColIndex])) {
					page[j] = tuple;
					writer.writePage(table.getPages().get(i).getPath(), page);
				}
			}
		}
	}

	// searches linearly through all pages of the table to find the requested tuple
	// returns header and tuple
	public static String[][] findTuple(Table table, String strClusteringKeyValue) throws DBAppException {
		String[][] res;
		for (int i = 0; i < table.getPages().size(); i++) {
			String[][] page = reader.readCSV(table.getPages().get(i).getPath());
			res = new String[page.length][2];
			int clusterColIndex = 0;
			for (int h = 0; h < page[0].length; h++) {
				if (table.getClusterCol().equals(page[0][h]))
					clusterColIndex = h;
			}
			for (int j = 1; j < page.length; j++) {
				if (strClusteringKeyValue.equals(page[j][clusterColIndex])) {
					res[0] = page[0];
					res[1] = page[j];
					return res;
				}
			}
		}
		throw new DBAppException("Tuple not found in Table");
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (!tables.containsKey(strTableName))
			throw new DBAppException(strTableName + " Table does not exist");
		try {
			Table curTable = tables.get(strTableName);
			if (curTable.hasIndex()) {
				// if there is an index created on the table required
			} else {
				// if no index is created on the table
				// search linearly through the entire table to find the required tuple/tuples
				for (int i = 0; i < curTable.getPages().size(); i++) {
					String[][] page = reader.readCSV(curTable.getPages().get(i).getPath());
					String[] header = page[0];
					int clusterColIndex = 0;
					for (int h = 0; h < header.length; h++) {
						if (curTable.getClusterCol().equals(header[h]))
							clusterColIndex = h;
					}
					for (int j = 1; j < page.length; j++) {
						// loop till i know which one is in the deletion list get the
						// index from it
						for (int k = 0; k < page[j].length; k++) {

							// need to check for all input columns before deletion

							if (htblColNameValue.containsKey(header[k])) {
								Class colClass = htblColNameValue.get(header[k]).getClass();
								Constructor constructor = colClass.getConstructor(String.class);
								Object val = constructor.newInstance(page[j][k]);
								if (htblColNameValue.get(header[k]).equals(val))
									shiftupTuples(curTable, i, j);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// shift tuples in the table for deletion method
	public static String[][] shiftupTuples(Table table, int pageNum, int index) {// <------------------doesnt make the
																					// last attribute
		// empty
		int Req_row = index;
		String[][] page = reader.readCSV(table.getPages().get(pageNum).getPath());
		if (page.length > 2) {
			// first reserved for the labels and the second for the first and only attribute
			// which will be deleted
			int j;
			for (j = Req_row; j < page.length - 1; j++) {// shift from the required row till the end
				page[j] = page[j + 1];
			}
			page[page.length - 1] = null;
			String filePath = table.getPages().get(pageNum).getPath();
			writer.writePage(filePath, page);
		} else {
			try {
				Path path = Paths.get(table.getPages().get(pageNum).getPath());
				Files.delete(path);
			} catch (IOException e) {
				System.out.println("An error occurred while deleting the file: " + e.getMessage());
			}
		}

		return null;
	}// <--------------------------------------------------------------------------------------------------------------------------------------------------------->

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		return null;
	}

	public static void printGrid(String[][] grid) {
		for (String[] row : grid) {
			if (row[0] == null)
				break;
			for (String element : row) {
				System.out.print(element + " ");
			}
			System.out.println();
		}
	}

	public static void printArray(String[] arr) {
		for (String s : arr) {
			System.out.print(s + " ");
		}
		System.out.println();
	}

	// converts an array to a String in csv format
	public static String convertToString(String[] arr) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < arr.length; i++) {
			sb.append(arr[i]);
			if (i != arr.length - 1)
				sb.append(",");
		}
		return sb.toString();
	}
}
