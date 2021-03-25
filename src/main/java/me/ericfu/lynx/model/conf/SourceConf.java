package me.ericfu.lynx.model.conf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import me.ericfu.lynx.source.random.RandomSourceConf;
import me.ericfu.lynx.source.text.TextSourceConf;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = TextSourceConf.class, name = "text"),
    @JsonSubTypes.Type(value = RandomSourceConf.class, name = "random"),
})
public interface SourceConf {

}
