package me.ericfu.lynx.source;

import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.model.conf.SourceConf;
import me.ericfu.lynx.source.random.RandomSource;
import me.ericfu.lynx.source.random.RandomSourceConf;
import me.ericfu.lynx.source.text.TextSource;
import me.ericfu.lynx.source.text.TextSourceConf;

public class SourceFactory {

    public Source create(GeneralConf globals, SourceConf conf) {
        if (conf instanceof TextSourceConf) {
            return new TextSource(globals, (TextSourceConf) conf);
        }
        if (conf instanceof RandomSourceConf) {
            return new RandomSource(globals, (RandomSourceConf) conf);
        }
        throw new IllegalStateException("unreachable");
    }

}
