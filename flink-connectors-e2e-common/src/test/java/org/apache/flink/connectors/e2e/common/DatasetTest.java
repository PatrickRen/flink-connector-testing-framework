package org.apache.flink.connectors.e2e.common;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class DatasetTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testCreateRandomText() {
		int numLine = 10;
		int lengthPerLine = 100;
		try {
			File randomTextFile = tempFolder.newFile();
			Dataset.writeRandomTextToFile(randomTextFile, numLine, lengthPerLine);
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
			Dataset.writeRandomBinaryToFile(randomBinaryFile, length);
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
			Dataset.writeRandomBinaryToFile(randomBinaryFile, length);
			File duplicatedBinaryFile = tempFolder.newFile();
			FileUtils.copyFile(randomBinaryFile, duplicatedBinaryFile);
			Assert.assertTrue(Dataset.isSame(randomBinaryFile, duplicatedBinaryFile));

			// Text file
			File randomTextFile = tempFolder.newFile();
			Dataset.writeRandomTextToFile(randomTextFile, numLine, lengthPerLine);
			File duplicatedTextFile = tempFolder.newFile();
			FileUtils.copyFile(randomTextFile, duplicatedTextFile);
			Assert.assertTrue(Dataset.isSame(randomTextFile, duplicatedTextFile));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
