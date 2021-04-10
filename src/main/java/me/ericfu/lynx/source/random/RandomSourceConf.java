package me.ericfu.lynx.source.random;

import lombok.Data;
import me.ericfu.lynx.model.conf.SourceConf;
import me.ericfu.lynx.schema.type.BasicType;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Positive;
import java.util.List;
import java.util.Map;

@Data
public class RandomSourceConf implements SourceConf {

    /**
     * Total number of records
     */
    @Positive
    private int records = 10000;

    /**
     * Generation rule of columns for each table
     * <p>
     * If a columns presented in sink schema but not presented here, a default generation rule will be chosen.
     */
    @NotEmpty
    private Map<String, List<ColumnSpec>> tables;

    @Data
    public static class ColumnSpec {

        /**
         * Column name
         */
        @NotEmpty
        private String name;

        /**
         * Column type
         */
        @NotEmpty
        private BasicType type;

        /**
         * A customized Java expression or code block generating a random value (optional)
         */
        private String rule;
    }
}
