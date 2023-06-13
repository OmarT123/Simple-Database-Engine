import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class GridIndex implements Serializable {
	private String name;
	private Table onTable;
	private String col1, col2, col1Type, col2Type;
	private String min1, max1, min2, max2;
	private boolean parsable = false;;
	private String[][] grid;
	private int gridSize;

	public GridIndex(String name, Table table, String colType1, String col1, String min1, String max1, String colType2,
			String col2, String min2, String max2) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		// verify table name
		// verify col names
		this.name = name;
		this.onTable = table;
		this.col1 = col1;
		this.col2 = col2;
		this.col1Type = col1Type;
		this.col2Type = col2Type;
		
		int N = 10; // store grid size in config file
		this.grid = new String[N + 1][N + 1];
		this.grid[0][0] = "     ";
		if (canParseInt(min1) && canParseInt(min2) && canParseInt(max1) && canParseInt(max2)) {
			int min1Int = Integer.parseInt(min1);
			int max1Int = Integer.parseInt(max1);
			int min2Int = Integer.parseInt(min2);
			int max2Int = Integer.parseInt(max2);

			int groupSize1 = (max1Int - min1Int) / N;
			int groupSize2 = (max2Int - min2Int) / N;

			int j = min1Int;
			for (int i = 1; i < this.grid.length; i++) {
				String group = "" + j;
				j += groupSize1;
				j = Math.min(j, max1Int);
				group += "-" + j;
				this.grid[0][i] = group;
				j++;
			}
			j = 0;
			for (int i = 1; i < this.grid.length; i++) {
				String group = "" + j;
				j += groupSize2;
				j = Math.min(j, max2Int);
				group += "-" + j;
				this.grid[i][0] = group;
				j++;
			}

		} else {

			// not working yet

			Class colClass1 = Class.forName(colType1);
			Class colClass2 = Class.forName(colType2);
			Constructor const1 = colClass1.getConstructor(String.class);
			Constructor const2 = colClass2.getConstructor(String.class);

			Object min1Obj = const1.newInstance(min1);
			Object max1Obj = const1.newInstance(max1);
			Object min2Obj = const1.newInstance(min2);
			Object max2Obj = const1.newInstance(max2);

			// fill headers
			int groupSize1 = 0;
			int groupSize2 = (((Comparable) max2Obj).compareTo(min2Obj) + 1) / (N);

			System.out.println(((Comparable) max1Obj).compareTo(min1Obj));
			System.out.println(groupSize1);
			System.out.println(groupSize2);

			Object j = min1Obj;
			int i = 1;
			for (j = min1Obj; i <= N && ((Comparable) j).compareTo(max1Obj) < 0; incrementObject(j, 1)) {
				String group = j.toString();
				incrementObject(j, groupSize1);
				if (((Comparable) j).compareTo(max1Obj) > 0)
					j = max1Obj;
				group += "-" + j.toString();
				this.grid[0][i++] = group;
			}
			i = 1;
			for (j = min2Obj; i <= N && ((Comparable) j).compareTo(max2Obj) < 0; incrementObject(j, 1)) {
				String group = j.toString();
				incrementObject(j, groupSize2);
				if (((Comparable) j).compareTo(max2Obj) > 0)
					j = max2Obj;
				group += "-" + j.toString();
				this.grid[i++][0] = group;
			}
		}
		String[][] metaData = Reader.readCSV("metadata.csv");
		for (int k = 1; k < metaData.length; k++) {
			if (metaData[k][0].equals(onTable.getName())) {
				if (metaData[k][1].equals(col1) || metaData[k][1].equals(col2)) {
					metaData[k][4] = name;
					metaData[k][5] = "GridIndex";
				}
			}
		}
		Writer.writePage("metadata.csv", metaData);
		
		// if there are tuples in the table
		// loop over all of them to fill the grid index
		
		ArrayList<String[]> column1 = new ArrayList<>();
		ArrayList<String[]> column2 = new ArrayList<>();
		for (int i = 1; i < this.grid.length; i++) {
			String[] minMaxPair = grid[0][i].split("-");
			column1.add(minMaxPair);
			minMaxPair = grid[i][0].split("-");
			column2.add(minMaxPair);
		}
		Class colClass1 = Class.forName(colType1);
		Class colClass2 = Class.forName(colType2);
		Constructor constructor1 = colClass1.getConstructor(String.class);
		Constructor constructor2 = colClass2.getConstructor(String.class);
		
		for (int i = 0; i < onTable.getPages().size(); i++) {
			int indexedRow;
			int indexedColumn;
			String[][] page = Reader.readCSV(onTable.getPages().get(i).getPath());
			String[] header = page[0];
			int col1Index = 0;
			int col2Index = 0;
			for (int j = 0; j < header.length; j++) {
				if (col1.equals(header[j]))
					col1Index = j;
				if (col2.equals(header[j]))
					col2Index = j;
			}
			for (int j = 1; j < page.length; j++) {
				Object col1Val = constructor1.newInstance(page[j][col1Index]);
				Object col2Val = constructor2.newInstance(page[j][col2Index]);
				int[] indexInGrid = new int[2];
				int index = 0;
				while (index < column1.size()) {
					Object max1Val = constructor1.newInstance(column1.get(index)[1]);
					Object max2Val = constructor2.newInstance(column2.get(index)[1]);
					Object min1Val = constructor1.newInstance(column1.get(index)[0]);
					Object min2Val = constructor2.newInstance(column2.get(index)[0]);
					if (((Comparable) max1Val).compareTo(col1Val) >= 0 && ((Comparable) min1Val).compareTo(col1Val) <= 0) {
						indexInGrid[0] = index + 1;
					}
					if (((Comparable) max2Val).compareTo(col2Val) >= 0 && ((Comparable) min2Val).compareTo(col2Val) <= 0) {
						indexInGrid[1] = index + 1;
					}
					index++;
				}
				
//				System.out.println("row: " + indexInGrid[1]);
//				System.out.println("col: " + indexInGrid[0]);
				if (indexInGrid[0] == 0)
					System.out.println(onTable.getPages().get(i).getName());
				
				if (grid[indexInGrid[1]][indexInGrid[0]] == null)
					grid[indexInGrid[1]][indexInGrid[0]] = onTable.getPages().get(i).getName();
				else if (!grid[indexInGrid[1]][indexInGrid[0]].contains(onTable.getPages().get(i).getName()))
					grid[indexInGrid[1]][indexInGrid[0]] += "-" + onTable.getPages().get(i).getName();	
			}
		}
//		for (int a = 0; a < grid.length; a++) {
//			for (int b = 0; b < grid[a].length; b++) {
//				System.out.print(grid[a][b] + " ");
//			}
//			System.out.println();
//		}
		saveIndex();
		// fill data if available

	}

	public static boolean canParseInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Table getOnTable() {
		return onTable;
	}

	public String getCol1() {
		return col1;
	}

	public String getCol2() {
		return col2;
	}

	public String getMin1() {
		return min1;
	}

	public String getMax1() {
		return max1;
	}

	public String getMin2() {
		return min2;
	}

	public String getMax2() {
		return max2;
	}

	public boolean isParsable() {
		return parsable;
	}

	public void setParsable(boolean parsable) {
		this.parsable = parsable;
	}

	public String[][] getGrid() {
		return grid;
	}

	public void setGrid(String[][] grid) {
		this.grid = grid;
	}

	public static Object incrementObject(Object obj, Object amount) {
		Class curClass = obj.getClass();
		if (obj instanceof Integer) {
			Integer cur = ((Integer) obj);
			Integer am = ((Integer) amount);
			return cur + am;
		} else if (obj instanceof Double) {
			Double cur = ((Double) obj);
			Double am = ((Double) amount);
			return cur + am;
		} else if (obj instanceof String) {
			String cur = (String) obj;
			Integer am = (Integer) amount;
			for (int i = 0; i < am; i++) {
				cur = incrementLexicographically(cur);
			}
			return cur;
		} else {
		}
		return null;
	}

	public static String incrementLexicographically(String input) {
		char[] chars = input.toCharArray();
		int length = chars.length;

		for (int i = length - 1; i >= 0; i--) {
			if (chars[i] != 'z') {
				chars[i]++;
				for (int j = i + 1; j < length; j++) {
					chars[j] = 'a';
				}
				return new String(chars);
			}
		}
		return input;
	}
	
	// Not needed (Only for testing)
	public void saveIndex() {
		String filePath = getName() + ".csv";
		Writer.writePage(filePath, getGrid());
	}

	// methods: insert, get, remove, contains
	
	public void insert(String[] tuple, String pageName) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		ArrayList<String[]> column1 = new ArrayList<>();
		ArrayList<String[]> column2 = new ArrayList<>();
		for (int i = 1; i < this.grid.length; i++) {
			String[] minMaxPair = grid[0][i].split("-");
			column1.add(minMaxPair);
			minMaxPair = grid[i][0].split("-");
			column2.add(minMaxPair);
		}
		
		Class colClass1 = Class.forName(col1Type);
		Class colClass2 = Class.forName(col2Type);
		Constructor constructor1 = colClass1.getConstructor(String.class);
		Constructor constructor2 = colClass2.getConstructor(String.class);
		
			int indexedRow;
			int indexedColumn;
			String[][] page = Reader.readCSV(onTable.getPages().get(0).getPath());
			String[] header = page[0];
			int col1Index = 0;
			int col2Index = 0;
			for (int j = 0; j < header.length; j++) {
				if (col1.equals(header[j]))
					col1Index = j;
				if (col2.equals(header[j]))
					col2Index = j;
			}
			
				Object col1Val = constructor1.newInstance(tuple[col1Index]);
				Object col2Val = constructor2.newInstance(tuple[col2Index]);
				int[] indexInGrid = new int[2];
				int index = 0;
				while (index < column1.size()) {
					Object cur1 = constructor1.newInstance(column1.get(index)[1]);
					Object cur2 = constructor2.newInstance(column2.get(index)[1]);
					if (((Comparable) cur1).compareTo(col1Val) >= 0) {
						indexInGrid[0] = index + 1;
					}
					if (((Comparable) cur2).compareTo(col2Val) >= 0) {
						System.out.println("entered");
						indexInGrid[1] = index + 1;
					}
					index++;
					break;
				}
				
//				if (grid[indexInGrid[1]][indexInGrid[0]] == null)
//					grid[indexInGrid[1]][indexInGrid[0]] = onTable.getPages().get(i).getName();
//				else if (!grid[indexInGrid[1]][indexInGrid[0]].contains(onTable.getPages().get(i).getName()))
//					grid[indexInGrid[1]][indexInGrid[0]] += "," + onTable.getPages().get(i).getName();	
			
		
	}
}
