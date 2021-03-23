package me.ericfu.lightning.sink.jdbc;

import me.ericfu.lightning.conf.SinkConf;
import me.ericfu.lightning.exception.InvalidConfigException;

public class JdbcSinkConf extends SinkConf {

    private String url;
    private String user;
    private String password;
    private String table;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public void validate() throws InvalidConfigException {
        if (url == null) {
            throw new InvalidConfigException("url should not be null");
        } else if (table == null) {
            throw new InvalidConfigException("table should not be null");
        }
    }
}
