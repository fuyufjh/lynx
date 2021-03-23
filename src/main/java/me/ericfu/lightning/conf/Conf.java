package me.ericfu.lightning.conf;

import me.ericfu.lightning.exception.InvalidConfigException;

public interface Conf {

    void validate() throws InvalidConfigException;

}
