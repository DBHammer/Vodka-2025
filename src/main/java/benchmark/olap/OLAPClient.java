package benchmark.olap;

import benchmark.olap.query.baseQuery;
import benchmark.oltp.OLTPClient;
import benchmark.oltp.schedule.OnlineDDLManager;
import config.CommonConfig;
import org.apache.log4j.Logger;

import bean.Order;
import bean.OrderLine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;

public class OLAPClient {
    private static Logger log = Logger.getLogger(OLAPClient.class);
    private static Integer queryNumber = 22;
    public static double[] filterRate = { 0.700, 1, 0.3365, 0.2277, 0.3040,
            0.2098, 0.3156, 0.4555, 1, 0.266,
            1, 0.2054, 1, 0.3857, 0.4214,
            1, 1, 1, 1, 0.2098,
            1, 1, 1 };

    public static void initFilterRatio(String database, Properties dbProps, int dbType, boolean isScaleFilterRatio,
            double scaleFilterRatio) {
        log.info("Initiating filter rate ...");
        try {
            Connection con = DriverManager.getConnection(database, dbProps);
            Statement stmt = con.createStatement();
            if (dbType == CommonConfig.DB_POSTGRES) {
                stmt.execute("SET max_parallel_workers_per_gather = 256;");
            } else if (dbType == CommonConfig.DB_OPENGAUSS) {
                stmt.execute("SET query_dop = 64;");
                // stmt.execute("SET enable_imcsscan=on;");
                System.out.println("executing parallel for init");
            }
            ResultSet result;
            // if (OLTPClient.isOnlineDDL == false && OLTPClient.isBurst == false) {
            for (int i = 0; i < queryNumber; i++) {
                String filterSQLPath = "filterQuery/" + (i + 1) + ".sql";
                if (dbType == CommonConfig.DB_POSTGRES)
                    filterSQLPath = "filterQuery/pg/" + (i + 1) + ".sql";
                if (i == 0 || i == 2 || i == 3 | i == 4 || i == 5 || i == 6 || i == 7 || i == 9 || i == 11
                        || i == 13
                        || i == 14 || i == 19) {
                    String filterSQLQuery = OLAPClient.readSQL(filterSQLPath);
                    System.out.println("We are executing query: " + filterSQLQuery);
                    result = stmt.executeQuery(filterSQLQuery);
                    if (result.next())
                        filterRate[i] = Double.parseDouble(result.getString(1));
                } else
                    filterRate[i] = 1;
            }
            // }

            if (isScaleFilterRatio && scaleFilterRatio != 0.0) {
                System.out.println("We need to adjust scaleRatio");
                for (int i = 0; i < queryNumber; i++) {
                    double scaled = 0.0;
                    // if (i == 0 || i == 2 || i == 4 || i == 7) {
                    // scaled = 1 - (1 - filterRate[i]) * scaleFilterRatio;
                    // } else {
                    scaled = filterRate[i] * scaleFilterRatio;
                    // }
                    if (scaled < 0)
                        filterRate[i] = 0.05;
                    else
                        filterRate[i] = (scaled >= 0.95) ? 0.95 : scaled;
                }
            }
            for (double rate : filterRate) {
                System.out.println(rate);
            }

            System.out.println("We are executing query: select count(*) from vodka_oorder;");
            result = stmt.executeQuery("select count(*) from vodka_oorder;");
            if (result.next())
                baseQuery.orderOriginSize = Integer.parseInt(result.getString(1));
            System.out.println(
                    "We are executing query: select count(*) from vodka_order_line where ol_delivery_d IS NOT NULL;");
            result = stmt.executeQuery("select count(*) from vodka_order_line where ol_delivery_d IS NOT NULL;");
            if (result.next())
                baseQuery.olNotnullSize = Integer.parseInt(result.getString(1));
            System.out.println("We are executing query: select count(*) from vodka_order_line;");
            result = stmt.executeQuery("select count(*) from vodka_order_line;");
            if (result.next())
                baseQuery.olOriginSize = Integer.parseInt(result.getString(1));
            System.out.println(
                    "We are executing query: select ol_delivery_d from vodka_order_line where ol_delivery_d IS NOT NULL;");
            result = stmt.executeQuery(
                    "select ol_delivery_d from vodka_order_line where ol_delivery_d IS NOT NULL;");
            System.out.println("ol_delivery_d can be get");
            if (OLTPClient.isSampling) {
                int index = 0;
                int batchSize = 1000000; // 设置批量输出的大小
                while (result.next()) {
                    Timestamp timestamp1 = result.getTimestamp(1);
                    OrderLine orderLine = new OrderLine(timestamp1);
                    OLTPClient.sampler4OrderLine.add(orderLine);
                    if (++index % batchSize == 0) {
                        System.out.println("OrderLine: current sampling index #" + ++index);
                    }
                }
                OLTPClient.sampler4OrderLine.sortForInitialize();
                System.out.println(
                        "We are executing query: select o_entry_d from vodka_oorder;");
                result = stmt.executeQuery(
                        "select o_entry_d from vodka_oorder;");
                index = 0;
                batchSize = 100000; // 设置批量输出的大小
                while (result.next()) {
                    Timestamp timestamp1 = result.getTimestamp(1);
                    Order order = new Order(timestamp1);
                    OLTPClient.sampler4Order.add(order);
                    if (++index % batchSize == 0) {
                        System.out.println("Order: current sampling index #" + ++index);
                    }
                }
                OLTPClient.sampler4Order.sortForInitialize();
                System.out.println("Data Prepare Done.");
            }

            if (OLTPClient.isOnlineDDL) {
                OnlineDDLManager.INSTANCE.preRunDDL();
            }
            result.close();
            stmt.close();
            con.close();
            System.out.println("Print Filter Rate Array Value: ");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        log.info("Filter rate initialization done.");
    }

    static String readSQL(String path) throws IOException {
        String result;
        StringBuilder builder = new StringBuilder();
        File file = new File(".", path);
        InputStreamReader streamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(streamReader);
        while ((result = bufferedReader.readLine()) != null) {
            builder.append(result);
            builder.append(" ");
        }
        bufferedReader.close();
        return builder.toString();
    }
}