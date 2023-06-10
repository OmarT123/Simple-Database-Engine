import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Writer {
	//writes content to the end of the  file provided
	public static void appendToFile(String fileName, String content) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
			// Writing content to the file
			writer.write(content);
			writer.newLine();
			//System.out.println("Data apppended to CSV file successfully.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//clears the file and writes content into it
	public static void overwriteFile(String fileName, String content) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
			// Writing data to the CSV file
			writer.write(content);
			writer.newLine();
			//System.out.println("Data written to CSV file successfully.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//Writes an entire Page
	public static void writePage(String fileName, String[][] page) {
		StringBuilder content = new StringBuilder();
		for (int i = 0; i < page.length; i++) {
			if (page[i] == null || page[i][0] == null)
				break;
			for (int j = 0; j < page[i].length; j++) {
				content.append(page[i][j]);
				content.append(",");
			}
			content.append("\n");
		}
		content.deleteCharAt(content.length() - 1);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
			// Writing data to the CSV file
			writer.write(content.toString());
			writer.newLine();
			//System.out.println("Page written to CSV file successfully.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}