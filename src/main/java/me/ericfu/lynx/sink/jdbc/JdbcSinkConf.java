package me.ericfu.lynx.sink.jdbc;

import lombok.Data;
import me.ericfu.lynx.model.conf.SinkConf;
import me.ericfu.lynx.source.jdbc.JdbcSourceConf.IdentifierQuotation;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Data
public class JdbcSinkConf implements SinkConf {

    @NotEmpty
    private String url;

    private String user;

    private String password;

    /**
     * Addition JDBC connection properties
     */
    private Map<String, String> properties;

    /**
     * How to deal with identifier?
     */
    @NotNull
    private IdentifierQuotation quoteIdentifier = IdentifierQuotation.NO_QUOTE;

}
