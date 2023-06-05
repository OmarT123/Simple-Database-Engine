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
			System.out.println("Data apppended to CSV file successfully.");
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
			System.out.println("Data written to CSV file successfully.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}