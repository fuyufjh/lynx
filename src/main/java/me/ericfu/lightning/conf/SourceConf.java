package me.ericfu.lightning.conf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import me.ericfu.lightning.source.random.RandomSourceConf;
import me.ericfu.lightning.source.text.TextSourceConf;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = TextSourceConf.class, name = "text"),
    @JsonSubTypes.Type(value = RandomSourceConf.class, name = "random"),
})
public abstract class SourceConf extends Conf {

}
