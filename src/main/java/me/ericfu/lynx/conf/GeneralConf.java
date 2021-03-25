package me.ericfu.lynx.conf;

import lombok.Data;
import org.hibernate.validator.constraints.Range;

@Data
public final class GeneralConf {

    @Range(min = 1, max = 10000000)
    private int batchSize = 10000;

    @Range(min = 1, max = 1024)
    private int threads = 16;

}
