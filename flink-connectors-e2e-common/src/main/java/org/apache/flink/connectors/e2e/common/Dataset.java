package org.apache.flink.connectors.e2e.common;

import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Random;

public class Dataset {

	private static final int MAX_BUF_SIZE = 8192;

	public static void writeRandomBinaryToFile(File file, int size) throws Exception {
		FileOutputStream fos = new FileOutputStream(file);
		byte[] buf = new byte[MAX_BUF_SIZE];
		Random random = new Random();
		int remain = size;
		while (remain > 0) {
			random.nextBytes(buf);
			int writeLen = remain > MAX_BUF_SIZE ? MAX_BUF_SIZE : remain;
			fos.write(buf, 0, writeLen);
			remain -= writeLen;
		}
		fos.close();
	}

	public static void writeRandomTextToFile(File file, int numLine, int lengthPerLine) throws Exception {
		String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
				"abcdefghijklmnopqrstuvwxyz" +
				"0123456789";
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw, MAX_BUF_SIZE);
		Random random = new Random();
		for (int i = 0; i < numLine; i++) {
			for (int j = 0 ; j < lengthPerLine; j++) {
				bw.write(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
			}
			bw.newLine();
		}
		bw.close();
		fw.close();
	}

	public static boolean isSame(File first, File second) throws Exception {
		return FileUtils.contentEquals(first, second);
	}
}
