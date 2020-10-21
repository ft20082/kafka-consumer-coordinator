package com.myteek.kafka.client;

import com.myteek.kafka.base.Constants;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;


public class KCClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KCClient.class);

    private String connectString;

    private String rootPath;

    private int sessionTimeout;

    private Watcher watcher;

    private RetryPolicy retryPolicy;

    private ZooKeeper zookeeper;

    private int retryNum = 0;

    public KCClient(String connectString) {
        this(connectString, Constants.DEFAULT_ZOOKEEPER_PATH);
    }

    public KCClient(String connectString, String rootPath) {
        this(connectString, rootPath, Constants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
    }

    public KCClient(String connectString, String rootPath, int sessionTimeout) {
        this(connectString, rootPath, sessionTimeout, new KCWatcher());
    }

    public KCClient(String connectString, String rootPath, int sessionTimeout, Watcher watcher) {
        this(connectString, rootPath, sessionTimeout, watcher, new RetryThreeTimes());
    }

    public KCClient(String connectString, String rootPath, int sessionTimeout, Watcher watcher,
                    RetryPolicy retryPolicy) {
        this.connectString = connectString;
        this.rootPath = rootPath;
        this.sessionTimeout = sessionTimeout;
        this.watcher = watcher;
        this.retryPolicy = retryPolicy;
        initRootPath();
    }

    public String getRootPath() {
        return rootPath;
    }

    /**
     * init zookeeper connection and root path
     * @throws Exception
     */
    private synchronized ZooKeeper.States initZookeeper() throws Exception {
        if (zookeeper == null) {
            zookeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
        }
        return zookeeper.getState();
    }

    public boolean initRootPath() {
        boolean ret = false;
        try {
            Stat root = exists(rootPath, watcher);
            if (root == null) {
                String createdPath = create(rootPath, null, CreateMode.PERSISTENT);
                if (createdPath != null) {
                    ret = true;
                    log.info("current thread: {}, zookeeper root path created. path: {}", Thread.currentThread(), createdPath);
                } else {
                    log.error("current thread: {}, zookeeper root path create error.", Thread.currentThread());
                }
            } else {
                ret = true;
                log.info("current thread: {}, zookeeper root path exists. info: {}", Thread.currentThread(), root);
            }
        } catch (Exception e) {
            log.error("current thread: {}, init zookeeper path error.", Thread.currentThread(), e);
        }
        return ret;
    }

    public Stat exists(String path, Watcher itemWatcher) {
        try {
            Optional<ZooKeeper> zk = getZookeeper();
            if (zk.isPresent()) {
                return zk.get().exists(path, itemWatcher);
            }
        } catch (Exception e) {
            log.error("current thread: {}, exists zookeeper path error.", Thread.currentThread(), e);
        }
        return null;
    }

    public String create(String path, byte[] data, CreateMode createMode) {
        try {
            Optional<ZooKeeper> zk = getZookeeper();
            if (zk.isPresent()) {
                return zk.get().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            }
        } catch (Exception e) {
            log.error("current thread: {}, create path error, path: {}, data: {}, createMode: {}",
                    Thread.currentThread(), path, data, createMode, e);
        }
        return null;
    }

    /**
     * watch current path delete, children path create / delete
     * @param path
     * @param itemWatcher
     * @param stat
     * @return
     */
    public List<String> getChildren(String path, Watcher itemWatcher, Stat stat) {
        try {
            Optional<ZooKeeper> zk = getZookeeper();
            if (zk.isPresent()) {
                return zk.get().getChildren(path, itemWatcher, stat);
            }
        } catch (Exception e) {
            log.error("get zookeeper path children error. path: {}", path, e);
        }
        return null;
    }

    public byte[] getData(String path, Watcher itemWatcher, Stat stat) {
        byte[] ret = new byte[0];
        try {
            Optional<ZooKeeper> zk = getZookeeper();
            if (zk.isPresent()) {
                ret = zk.get().getData(path, itemWatcher, stat);
            }
        } catch (Exception e) {
            log.error("get zookeeper path data error. path: {}", path, e);
        }
        return ret;
    }

    public boolean setData(String path, byte[] bytes, int version) {
        try {
            Optional<ZooKeeper> zk = getZookeeper();
            if (zk.isPresent()) {
                Stat stat = zk.get().setData(path, bytes, version);
                log.debug("set zookeeper data: {}, stat: {}", bytes, stat);
                return stat != null;
            }
        } catch (Exception e) {
            log.error("set zookeeper data error.", e);
        }
        return false;
    }

    /**
     * get zookeeper alive client
     * check isAlive, if not alive, need reconnect
     * if is not connected, need wait
     * @return
     */
    public Optional<ZooKeeper> getZookeeper() {
        try {
            ZooKeeper.States states = initZookeeper();
            while (!states.isAlive()) {
                log.debug("zookeeper connection retry num: {}", retryNum);
                if (retryPolicy.allowRetry(retryNum)) {
                    retryNum ++;
                    states = initZookeeper();
                } else {
                    break;
                }
            }
            log.debug("get current zookeeper states: {}", states);
            return states.isAlive() ? Optional.of(zookeeper) : Optional.empty();
        } catch (Exception e) {
            log.error("get zookeeper error. info: {}", zookeeper);
        }
        return Optional.empty();
    }

    /**
     * test is connected
     * @return
     */
    public boolean isConnected() {
        Optional<ZooKeeper> zk = getZookeeper();
        return zk.isPresent() ? zk.get().getState().isConnected() : false;
    }

    /**
     * test is alive
     * @return
     */
    public boolean isAlive() {
        Optional<ZooKeeper> zk = getZookeeper();
        return zk.isPresent() ? zk.get().getState().isAlive() : false;
    }

    @Override
    public void close() throws Exception {
        if (zookeeper != null) {
            zookeeper.close();
        }
    }

    @Override
    public String toString() {
        return "KCClient{" +
                "connectString='" + connectString + '\'' +
                ", rootPath='" + rootPath + '\'' +
                ", sessionTimeout=" + sessionTimeout +
                ", watcher=" + watcher +
                ", retryPolicy=" + retryPolicy +
                ", zookeeper=" + zookeeper +
                ", retryNum=" + retryNum +
                '}';
    }
}
