package org.apache.flink.connectors.e2e.common;

import org.apache.commons.io.FileUtils;
import org.apache.flink.connectors.e2e.common.util.TestDataHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class TestDataHelperTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testCreateRandomText() {
		int numLine = 10;
		int lengthPerLine = 100;
		try {
			File randomTextFile = tempFolder.newFile();
			TestDataHelper.writeRandomTextToFile(randomTextFile, numLine, lengthPerLine);
			Assert.assertEquals(numLine * (lengthPerLine + 1), randomTextFile.length());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCreateRandomBinary() {
		int length = 100;
		try {
			File randomBinaryFile = tempFolder.newFile();
			TestDataHelper.writeRandomBinaryToFile(randomBinaryFile, length);
			Assert.assertEquals(length, randomBinaryFile.length());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testFileComparing() {
		int length = 100;
		int numLine = 10;
		int lengthPerLine = 100;
		try {
			// Binary file
			File randomBinaryFile = tempFolder.newFile();
			TestDataHelper.writeRandomBinaryToFile(randomBinaryFile, length);
			File duplicatedBinaryFile = tempFolder.newFile();
			FileUtils.copyFile(randomBinaryFile, duplicatedBinaryFile);
			Assert.assertTrue(TestDataHelper.isSame(randomBinaryFile, duplicatedBinaryFile));

			// Text file
			File randomTextFile = tempFolder.newFile();
			TestDataHelper.writeRandomTextToFile(randomTextFile, numLine, lengthPerLine);
			File duplicatedTextFile = tempFolder.newFile();
			FileUtils.copyFile(randomTextFile, duplicatedTextFile);
			Assert.assertTrue(TestDataHelper.isSame(randomTextFile, duplicatedTextFile));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testAppendEndMark() {
		final String originalContent = "testtest";
		final String endMark = "EOF";
		final int bufferSize = 100;
		try {

			// Test file without empty new line at the end
			File fileWithoutTrailingNewline = tempFolder.newFile();
			FileWriter fw = new FileWriter(fileWithoutTrailingNewline);
			BufferedWriter bf = new BufferedWriter(fw);
			bf.write(originalContent);
			bf.close();
			fw.close();

			TestDataHelper.appendMarkToFile(fileWithoutTrailingNewline, endMark);

			FileReader fr = new FileReader(fileWithoutTrailingNewline);
			BufferedReader br = new BufferedReader(fr);
			char[] buf = new char[bufferSize];
			int numBytesRead = br.read(buf);
			br.close();
			fr.close();

			Assert.assertEquals(originalContent.length() + endMark.length() + "\n".length(), numBytesRead);
			Assert.assertEquals(originalContent + "\n" + endMark, new String(buf, 0, numBytesRead));

			// Test file with empty new line at the end
			File fileWithTrailingNewline = tempFolder.newFile();
			fw = new FileWriter(fileWithTrailingNewline);
			bf = new BufferedWriter(fw);
			bf.write(originalContent + "\n");
			bf.close();
			fw.close();

			TestDataHelper.appendMarkToFile(fileWithTrailingNewline, endMark);

			fr = new FileReader(fileWithTrailingNewline);
			br = new BufferedReader(fr);
			numBytesRead = br.read(buf);
			br.close();
			fr.close();

			Assert.assertEquals(originalContent.length() + endMark.length() + "\n".length() * 2, numBytesRead);
			Assert.assertEquals(originalContent + "\n" + endMark + "\n", new String(buf, 0, numBytesRead));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
