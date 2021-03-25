package me.ericfu.lynx.sink.jdbc;

import lombok.Data;
import me.ericfu.lynx.conf.SinkConf;

import javax.validation.constraints.NotEmpty;

@Data
public class JdbcSinkConf implements SinkConf {

    @NotEmpty
    private String url;

    private String user;

    private String password;

    @NotEmpty
    private String table;

}
