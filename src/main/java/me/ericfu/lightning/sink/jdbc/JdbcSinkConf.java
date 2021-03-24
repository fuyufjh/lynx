package me.ericfu.lightning.sink.jdbc;

import me.ericfu.lightning.conf.SinkConf;

import javax.validation.constraints.NotEmpty;

public class JdbcSinkConf extends SinkConf {

    @NotEmpty
    private String url;

    private String user;

    private String password;

    @NotEmpty
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
}
