package me.ericfu.lynx.sink.jdbc;

import lombok.Data;
import me.ericfu.lynx.model.conf.SinkConf;

import javax.validation.constraints.NotEmpty;
import java.util.Map;

@Data
public class JdbcSinkConf implements SinkConf {

    @NotEmpty
    private String url;

    private String user;

    private String password;

    private Map<String, String> properties;

}
