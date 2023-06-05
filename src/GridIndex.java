
public class GridIndex {
	private String name;
	private String onTable;
	private String col1, col2;
	private String min1, max1, min2, max2;
	private boolean parsable = false;;
	private String[][] grid;
	
	public GridIndex(String name, String tableName, String col1,String min1, String max1, String col2, String min2, String max2) {
		//verify table name
		//verify col names
		this.name = name;
		this.onTable = tableName;
		this.col1 = col1;
		this.col2 = col2;
		if (canParseInt(min1) && canParseInt(max1) && canParseInt(min2) && canParseInt(max2)) {
			parsable = true;
			int min1Int = Integer.parseInt(min1);
			int max1Int = Integer.parseInt(max1);
			int min2Int = Integer.parseInt(min2);
			int max2Int = Integer.parseInt(max2);
			this.grid = new String[Math.min(11, max1Int - min1Int + 1)][Math.min(11, max2Int - min2Int + 1)];
			this.grid[0][0] = "     ";
			//fill headers
			int groupSize1 = (max1Int - min1Int + 1) / (this.grid[0].length - 1);
			int groupSize2 = (max2Int - min2Int + 1) / (this.grid.length - 1);
			int j = 0;
			for (int i = 1; i < this.grid[0].length; i++) {
				String group = "" + j;
				j += groupSize1;
				j = Math.min(j, max1Int - min1Int);
				group += "-" + j;
				this.grid[0][i] = group;
				j++;
			}
			j = 0;
			for (int i = 1; i < this.grid.length; i++) {
				String group = "" + j;
				j += groupSize1;
				j = Math.min(j, max2Int - min2Int);
				group += "-" + j;
				this.grid[i][0] = group;
				j++;
			}
			//fill data if available
		}
		else {
			
		}
		//loop to store values of headers
		//for both columns that the index is created on
	}
	
	//Checks if string s can be parsed into an int
	public static boolean canParseInt(String s) {
		try {
			Integer.parseInt(s);
			return true;
		}
		catch (Exception e) {
			return false;
		}
	}

	// prints 2d array
	public void printIndex() {
		for (String[] row : grid) {
			for (String element : row) {
				System.out.print(element + " ");
			}
			System.out.println();
		}
	}

	public String[][] getGrid() {
		return grid;
	}
	//methods: inse`rt, get, remove, contains
	
	
}
