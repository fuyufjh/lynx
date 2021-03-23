package me.ericfu.lightning.sink;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.conf.SinkConf;
import me.ericfu.lightning.sink.jdbc.JdbcSink;
import me.ericfu.lightning.sink.jdbc.JdbcSinkConf;
import sun.plugin.dom.exception.InvalidStateException;

public class SinkFactory {

    public Sink create(GeneralConf globals, SinkConf conf) {
        if (conf instanceof JdbcSinkConf) {
            return new JdbcSink(globals, (JdbcSinkConf) conf);
        }
        throw new InvalidStateException("unreachable");
    }

}
