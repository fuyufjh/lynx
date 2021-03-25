package me.ericfu.lynx.sink;

import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.model.conf.SinkConf;
import me.ericfu.lynx.sink.jdbc.JdbcSink;
import me.ericfu.lynx.sink.jdbc.JdbcSinkConf;
import me.ericfu.lynx.sink.text.TextSink;
import me.ericfu.lynx.sink.text.TextSinkConf;
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
