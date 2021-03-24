package me.ericfu.lightning.sink;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.conf.SinkConf;
import me.ericfu.lightning.sink.jdbc.JdbcSink;
import me.ericfu.lightning.sink.jdbc.JdbcSinkConf;
import me.ericfu.lightning.sink.text.TextSink;
import me.ericfu.lightning.sink.text.TextSinkConf;
import sun.plugin.dom.exception.InvalidStateException;

public class SinkFactory {

    public Sink create(GeneralConf globals, SinkConf conf) {
        if (conf instanceof JdbcSinkConf) {
            return new JdbcSink(globals, (JdbcSinkConf) conf);
        } else if (conf instanceof TextSinkConf) {
            return new TextSink(globals, (TextSinkConf) conf);
        }
        throw new InvalidStateException("unreachable");
    }

}
