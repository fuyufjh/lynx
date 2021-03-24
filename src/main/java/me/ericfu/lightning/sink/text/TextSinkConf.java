package me.ericfu.lightning.sink.text;

import lombok.Data;

import javax.validation.constraints.NotEmpty;

@Data
public class TextSinkConf {

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
