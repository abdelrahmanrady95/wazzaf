/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Rady
 */
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import tech.tablesaw.api.Table;

import java.io.IOException;

public class FileLoader {

    public static Dataset<Row> load_csv(SparkSession session, String path, boolean header){
        System.out.println(header);
        return session.read()
                .option("header", header)
                .csv(path);
    }

    public static Table load_csv_table_saw(String path) throws IOException {
        return Table.read().file(path);
    }

}
