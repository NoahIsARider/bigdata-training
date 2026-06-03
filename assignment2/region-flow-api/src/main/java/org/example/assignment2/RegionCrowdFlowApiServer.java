package org.example.assignment2;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegionCrowdFlowApiServer {

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(envOrDefault("API_PORT", "8080"));
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/api/region-flow", new RegionFlowHandler());
        server.setExecutor(null);
        server.start();
        System.out.println("RegionCrowdFlowApiServer started on port " + port);
    }

    static class RegionFlowHandler implements HttpHandler {
        private static final byte[] FAMILY = Bytes.toBytes("crowds");

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                writeJson(exchange, 405, "{\"error\":\"method not allowed\"}");
                return;
            }

            Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
            String areaId = params.getOrDefault("areaId", "");
            String startHour = params.getOrDefault("startHour", "");
            String endHour = params.getOrDefault("endHour", "");

            if (areaId.isEmpty() || startHour.isEmpty() || endHour.isEmpty()) {
                writeJson(exchange, 400, "{\"error\":\"areaId,startHour,endHour are required\"}");
                return;
            }

            try {
                String json = queryRegionFlow(areaId, startHour, endHour);
                writeJson(exchange, 200, json);
            } catch (Exception ex) {
                String message = ex.getMessage() == null ? "unknown error" : ex.getMessage().replace("\"", "'");
                writeJson(exchange, 500, "{\"error\":\"" + message + "\"}");
            }
        }

        private String queryRegionFlow(String areaId, String startHour, String endHour) throws IOException {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", envOrDefault("HBASE_ZK_QUORUM", "hbase"));
            conf.set("hbase.zookeeper.property.clientPort", envOrDefault("HBASE_ZK_PORT", "2181"));
            conf.set("hbase.rootdir", envOrDefault("HBASE_ROOTDIR", "file:///opt/hbase-data/rootdir"));

            List<Map<String, String>> items = new ArrayList<Map<String, String>>();

            try (Connection connection = ConnectionFactory.createConnection(conf);
                 Table table = connection.getTable(TableName.valueOf("WZMF:TA_H_REGION_CROWD_FLOW"))) {

                Scan scan = new Scan();
                scan.addFamily(FAMILY);
                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result result : scanner) {
                        String rowAreaId = readString(result, "area_id");
                        String statHour = readString(result, "stat_hour");
                        if (!areaId.equals(rowAreaId)) {
                            continue;
                        }
                        if (statHour.compareTo(startHour) < 0 || statHour.compareTo(endHour) > 0) {
                            continue;
                        }

                        Map<String, String> item = new HashMap<String, String>();
                        item.put("statHour", statHour);
                        item.put("totalCount", readString(result, "total_count"));
                        item.put("inflowCount", readString(result, "inflow_count"));
                        item.put("stayCount", readString(result, "stay_count"));
                        item.put("outflowCount", readString(result, "outflow_count"));
                        items.add(item);
                    }
                }
            }

            items.sort(Comparator.comparing(o -> o.get("statHour")));

            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"areaId\":\"").append(areaId).append("\",");
            sb.append("\"startHour\":\"").append(startHour).append("\",");
            sb.append("\"endHour\":\"").append(endHour).append("\",");
            sb.append("\"items\":[");

            for (int i = 0; i < items.size(); i++) {
                Map<String, String> item = items.get(i);
                if (i > 0) {
                    sb.append(",");
                }
                sb.append("{");
                sb.append("\"statHour\":\"").append(item.get("statHour")).append("\",");
                sb.append("\"totalCount\":").append(zeroIfBlank(item.get("totalCount"))).append(",");
                sb.append("\"inflowCount\":").append(zeroIfBlank(item.get("inflowCount"))).append(",");
                sb.append("\"stayCount\":").append(zeroIfBlank(item.get("stayCount"))).append(",");
                sb.append("\"outflowCount\":").append(zeroIfBlank(item.get("outflowCount")));
                sb.append("}");
            }

            sb.append("]}");
            return sb.toString();
        }

        private String readString(Result result, String qualifier) {
            byte[] value = result.getValue(FAMILY, Bytes.toBytes(qualifier));
            return value == null ? "" : Bytes.toString(value);
        }

        private String zeroIfBlank(String value) {
            return value == null || value.isEmpty() ? "0" : value;
        }

        private Map<String, String> parseQuery(String query) throws IOException {
            Map<String, String> params = new HashMap<String, String>();
            if (query == null || query.isEmpty()) {
                return params;
            }
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] kv = pair.split("=", 2);
                String key = URLDecoder.decode(kv[0], StandardCharsets.UTF_8.name());
                String value = kv.length > 1 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8.name()) : "";
                params.put(key, value);
            }
            return params;
        }

        private void writeJson(HttpExchange exchange, int statusCode, String body) throws IOException {
            byte[] payload = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(statusCode, payload.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(payload);
            }
        }
    }
}
