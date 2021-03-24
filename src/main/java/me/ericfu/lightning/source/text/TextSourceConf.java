package me.ericfu.lightning.source.text;

import me.ericfu.lightning.conf.SourceConf;

import javax.validation.constraints.NotEmpty;

public class TextSourceConf extends SourceConf {

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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
