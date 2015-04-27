package com.symbolscope.gic.scratch;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.ResultSet;
import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class StressTestGremlin {
    public static class GremlinClient {
        Cluster cluster;
        Client client;


        public GremlinClient() {
            this.cluster = Cluster.open();
            this.client = cluster.connect();
        }

        public List<Result> op(String opName, Map<String, Object> params) throws Exception {
            RequestMessage.Builder rqmb = RequestMessage.build(opName).processor("scratch");
            for (Map.Entry<String, Object> e: params.entrySet()) {
                rqmb.addArg(e.getKey(), e.getValue());
            }
            RequestMessage rqm = rqmb.create();

            CompletableFuture<ResultSet> resultsFuture = client.submitAsync(rqm);
            ResultSet results = null;
            results = resultsFuture.get();
            return results.stream().collect(Collectors.toList());
        }

        public void close() {
            client.close();
            cluster.close();
        }
    }

    public static boolean checkStatusOp(long request, GremlinClient client) {
        boolean close = false;
        if (client == null) {
            client = new GremlinClient();
            close = true;
        }
        Exception err = null;
        try {
            List<Result> result = client.op("testOp", new HashMap<>());
            Result r1 = result.get(0);
            Map<String, Object> r1m = (Map<String, Object>) r1.getObject();
            String status = (String) r1m.get("status");
            if (!status.equals("ok")) {
                err = new Exception("status was not ok");
            }
        } catch (Exception e) {
            err = e;
        }
        if (close) {
            client.close();
        }
        if (err != null) {
            err.printStackTrace();
            System.out.println(String.format("Failed on op request #%d", request));
            return false;
        } else {
            return true;
        }
    }

    public static void main(String[] args) throws Exception {
        GremlinClient gc = new GremlinClient();
        boolean ok;
        for (long i = 0; i < 100000; i++) {
            ok = checkStatusOp(i, gc);
            if (!ok) {
                break;
            }
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }
        gc.close();
        System.out.println("Done");
    }
}
