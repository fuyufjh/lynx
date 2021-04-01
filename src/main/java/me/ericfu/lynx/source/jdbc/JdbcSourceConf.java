package me.ericfu.lynx.source.jdbc;

import lombok.Data;
import me.ericfu.lynx.model.conf.SourceConf;

import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Data
public class JdbcSourceConf implements SourceConf {

    @NotEmpty
    private String url;

    private String user;

    private String password;

    /**
     * Addition JDBC connection properties
     */
    private Map<String, String> properties;

    /**
     * Descriptions of tables to export
     */
    private Map<String, TableDesc> tables;

    /**
     * How to deal with identifier?
     */
    @NotNull
    private IdentifierQuotation quoteIdentifier = IdentifierQuotation.NO_QUOTE;

    @Data
    public static class TableDesc {

        /**
         * Selected columns to export. Leave it unset to export all columns
         */
        @Nullable
        private List<String> columns;

    }

    public enum IdentifierQuotation {
        /**
         * Do not quote identifier
         */
        NO_QUOTE,

        /**
         * Quote with quotation mark (")
         */
        DOUBLE,

        /**
         * Quote with single quotation mark (`)
         */
        SINGLE,

        /**
         * Quote with back-quote mark (`)
         */
        BACKQUOTE,
    }
}
