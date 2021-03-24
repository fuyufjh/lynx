package me.ericfu.lightning.conf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import me.ericfu.lightning.sink.jdbc.JdbcSinkConf;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = JdbcSinkConf.class, name = "jdbc"),
})
public interface SinkConf {

}
