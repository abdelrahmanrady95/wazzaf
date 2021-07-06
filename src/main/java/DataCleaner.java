/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Rady
 */
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

public class DataCleaner {

    public static String getNullCount(SparkSession session, Dataset<Row> df){
        StringBuilder builder = new StringBuilder();
        List<Long> cols = new ArrayList<>();
        df.createOrReplaceTempView("wuzzuf");
        for(String col : df.columns()) {
            long c =  session.sql("select "+col+" as "+col+" from wuzzuf where "+col+" is null").count();
            cols.add(c);
        }
        int count = 0;
        for (String col : df.columns()) {
            builder.append("<br>").append("<b>").append(col).append(": </b>").append(cols.get(count++)).append("</br>");
        }
        return builder.toString();
    }


    public static Dataset<Row> dropNulls(SparkSession session, Dataset<Row> df){
        return df.filter((Row row) -> {
            return !row.anyNull();
        });
    }

    public static Dataset<Row> removeDuplicates(SparkSession session, Dataset<Row> df){
        return df.distinct();
    }


}

