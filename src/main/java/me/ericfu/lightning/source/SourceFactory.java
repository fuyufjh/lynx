package me.ericfu.lightning.source;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.conf.SourceConf;
import me.ericfu.lightning.source.text.TextSource;
import me.ericfu.lightning.source.text.TextSourceConf;
import sun.plugin.dom.exception.InvalidStateException;

public class SourceFactory {

    public Source create(GeneralConf globals, SourceConf conf) {
        if (conf instanceof TextSourceConf) {
            return new TextSource(globals, (TextSourceConf) conf);
        }
        throw new InvalidStateException("unreachable");
    }

}
