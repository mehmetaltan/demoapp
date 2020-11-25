package essparksql.demoapp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws ParseException {

        String querydsl = "{\"query\":{\"term\":{\"country\":{\"value\":\"mexico\"}}}}";

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("appnametest").getOrCreate();
        Dataset<Row> dfElasticData = sparkSession.read().format("org.elasticsearch.spark.sql")
                .option("es.nodes", "127.0.0.1").option("es.port", "9200").option("es.resource", "testspark/_doc")
                .option("es.query", "?q=country:mexico").load();
        dfElasticData = dfElasticData.groupBy("type").count();
        dfElasticData.show();
    }
}
