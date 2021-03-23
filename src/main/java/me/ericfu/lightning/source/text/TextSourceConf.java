package me.ericfu.lightning.source.text;

import me.ericfu.lightning.conf.SourceConf;
import me.ericfu.lightning.exception.InvalidConfigException;

public class TextSourceConf extends SourceConf {

    /**
     * path to the directory or file
     */
    private String path;

    /**
     * column separator
     */
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

    @Override
    public void validate() throws InvalidConfigException {
        if (path == null) {
            throw new InvalidConfigException("path is required");
        }
    }
}
