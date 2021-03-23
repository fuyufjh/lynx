package me.ericfu.lightning.conf;

import me.ericfu.lightning.exception.InvalidConfigException;

public interface Conf {

    // TODO: leverage a validation framework
    void validate() throws InvalidConfigException;

}
