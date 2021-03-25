package me.ericfu.lynx.model.checkpoint;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface SinkCheckpoint {

}
