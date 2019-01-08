package com.lijiaqi.spark.java;

import java.io.*;
import java.net.URL;

/**
 * Title:sparklearn
 * description:
 *
 * @author: Lijiaqi
 * @version: 1.0
 * @create: 2018-11-29 10:57
 **/

public class URLTest {

    public static void main(String[] args) throws IOException {
        URL url = new URL("http://www.baidu.com");

        InputStreamReader inputStream = new InputStreamReader(url.openStream());

        BufferedReader br = new BufferedReader(inputStream);

        br.lines().forEach(System.out::println);

        br.close();

    }
}
