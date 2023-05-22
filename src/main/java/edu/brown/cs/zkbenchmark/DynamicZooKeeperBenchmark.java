package edu.brown.cs.zkbenchmark;

import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import org.apache.zookeeper.data.Stat;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.retry.RetryOneTime;

public class DynamicZooKeeperBenchmark {

    private static ArrayList<String> servers = new ArrayList<>();

    private static String replacementServer;

    private static String path = "/client";

    private static String data = "";

    private static volatile boolean finish = false;

    static {
        for (int i = 0; i < 20; i++) { // 100 bytes of important data
            data += "!!!!!";
        }
    }

    static class Worker implements Runnable {

        private volatile CuratorFramework client;

        private final int id;

        private volatile int windowOps = 0;

        private int totalOps = 0;

        private CuratorFramework setupClient(String server) throws Exception {
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(server).namespace("/zkTest")
                    .retryPolicy(new RetryNTimes(1000, 1000))
                    .connectionTimeoutMs(1000).build();
            client.start();
            client.getZookeeperClient().setRetryPolicy(new RetryOneTime(1000));
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    if (newState == ConnectionState.SUSPENDED && client != null) {
                        System.out.println(String.format("[worker=%s] Closing client %s", id, client));
                        String[] replacementIPPort = replacementServer.split(":");
                        try {
                            while(true) {
                                try (Socket ignored = new Socket(replacementIPPort[0], Integer.parseInt(replacementIPPort[1]))) {
                                    break;
                                } catch (ConnectException | UnknownHostException e) {
                                    Thread.sleep(50);
                                }
                            }
                            Worker.this.client.close();
                            Worker.this.client = setupClient(replacementServer);
                        } catch (Exception e) {
                            System.out.println(String.format("[worker=%s] EXCEPTION %s", id, e.getLocalizedMessage()));
                            e.printStackTrace();
                        }

                    }
                }
            });

            Stat stat = client.checkExists().forPath(path);
            if (stat == null) {
                client.create().forPath(path, data.getBytes());
            }

            System.out.println(String.format("[worker=%s] Adding server %s client %s", id, server, client));
            return client;
        }

        public Worker(int id) throws Exception {
            this.id = id;
            client = setupClient(servers.get(id));
        }

        @Override
        public void run() {
            while (!finish) {
                try {
                    System.out.println(String.format("[worker=%s server=%s req=%s] before", id, servers.get(id), totalOps));
                    client.getData().forPath(path);
                    System.out.println(String.format("[worker=%s server=%s req=%s] after", id, servers.get(id), totalOps));
                    totalOps++;
                    windowOps++;
                } catch (Exception e) {
                    System.out.println(String.format("[worker=%s] EXCEPTION %s", id, e.getLocalizedMessage()));
                    e.printStackTrace();
                }
            }
            System.out.println(String.format("[%s] Done!", id));
        }
    }

    public static void main(String[] args) throws Exception {
        ArrayList<Thread> threads = new ArrayList<>();
        ArrayList<Worker> workers = new ArrayList<>();
        int timeLimitMS = Integer.parseInt(args[0]);
        servers.add(args[1]);
        servers.add(args[2]);
        servers.add(args[3]);
        replacementServer = args[4];

        System.out.println(String.format("Experiment time = %s, servers = %s, replServer = %s", timeLimitMS, servers, replacementServer));

        for (int i = 0; i < servers.size(); i++) {
            Worker w = new Worker(i);
            Thread t = new Thread(w);
            t.start();
            workers.add(w);
            threads.add(t);
        }

        for(int timeMS = 0; timeMS < timeLimitMS; timeMS += 100) {
            Thread.sleep(100);
            int ops = 0;
            for (Worker w : workers) {
                ops += w.windowOps;
                System.out.println(String.format("[ time-%s ops/s %s %s ]", w.id, timeMS, w.windowOps * 10));
                w.windowOps = 0;
            }
            System.out.println(String.format("[ time ops/s %s %s ]", timeMS, ops * 10));
        }

        finish = true;

        for (Thread t : threads) {
            t.join();
        }

        System.exit(0);
    }
}