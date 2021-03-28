package me.ericfu.lynx.source.jdbc;

import lombok.Data;
import me.ericfu.lynx.model.conf.SourceConf;

import javax.validation.constraints.NotEmpty;
import java.util.List;
import java.util.Map;

@Data
public class JdbcSourceConf implements SourceConf {

    @NotEmpty
    private String url;

    private String user;

    private String password;

    private Map<String, String> properties;

    /**
     * Descriptions of tables to export
     */
    private Map<String, TableDesc> tables;

    @Data
    public static class TableDesc {

        /**
         * Selected columns to export
         */
        private List<String> columns;

        /**
         * Split key is used to split table into multiple splits. By default the integer primary key will be
         * chosen if exists
         */
        private String splitKey;

    }

}
