package com.symbolscope.gic.scratch;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.SerTokens;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class StressTestGremlin {
    public static class GremlinClient {
        Cluster cluster;
        Client client;

        public GremlinClient(Cluster cluster) {
            this.cluster = cluster;
            this.client = cluster.connect();
        }

        public GremlinClient() {
            this(Cluster.open());
        }

        public List<Result> op(String opName, Map<String, Object> params) throws Exception {
            RequestMessage.Builder rqmb = RequestMessage.build(opName).processor("scratch");
            for (Map.Entry<String, Object> e: params.entrySet()) {
                rqmb.addArg(e.getKey(), e.getValue());
            }
            RequestMessage rqm = rqmb.create();

            CompletableFuture<ResultSet> resultsFuture = client.submitAsync(rqm);
            ResultSet results = resultsFuture.get();
            return results.stream().collect(Collectors.toList());
        }

        public void newCluster() {
            client.close();
            cluster.close();
            cluster = Cluster.open();
            client = cluster.connect();
        }

        public void newClient() {
            client.close();
            client = cluster.connect();
        }

        public void shutdown() {
            cluster.close();
        }
    }

    public static boolean checkStatusOp(long request, GremlinClient client) {
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
        if (err != null) {
            //err.printStackTrace();
            return false;
        } else {
            return true;
        }
    }

    public static void runStressTest(long n, int id) {
        System.out.println(String.format("Thread %d starting. Will run %d ops", id, n));
        GremlinClient gc = new GremlinClient();
        boolean ok;
        for (long i = 0; i < n; i++) {
            gc.newClient();
            //gc.newCluster();
            ok = checkStatusOp(i, gc);
            if (!ok) {
                System.out.println(String.format(String.format("Thread %d Failed on op request #%d", id, i)));
                //break;
            }
            if (i % 100 == 0) {
                System.out.println(String.format("Thread %d: %d", id, i));
            }
        }
        gc.shutdown();
        System.out.println(String.format("Thread %d Done", id));
    }

    public static void main(String[] args) throws Exception {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            //changing this value versus gremlinPool or threadPoolWorker seems to change time to failure, but still fails when this is small and those are large.
            int id = i;
            Thread t = new Thread( new  Runnable() {

                @Override
                public void run() {
                    runStressTest(100000, id);
                }
            });
            t.start();
            threads.add(t);
        }
        for (Thread t: threads) {
            t.join();
        }
    }
}
