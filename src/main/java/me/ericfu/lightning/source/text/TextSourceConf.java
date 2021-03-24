package me.ericfu.lightning.source.text;

import lombok.Data;
import me.ericfu.lightning.conf.SourceConf;

import javax.validation.constraints.NotEmpty;

@Data
public class TextSourceConf implements SourceConf {

    /**
     * path to the directory or file
     */
    @NotEmpty
    private String path;

    /**
     * column separator
     */
    @NotEmpty
    private String separator = ",";

}
