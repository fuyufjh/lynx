package me.ericfu.lightning.sink.text;

import lombok.Data;
import me.ericfu.lightning.conf.SinkConf;

import javax.validation.constraints.NotEmpty;

@Data
public class TextSinkConf implements SinkConf {

    /**
     * a directory to save the exported data
     */
    @NotEmpty
    private String path;

    /**
     * column separator
     */
    @NotEmpty
    private String separator = ",";

}
