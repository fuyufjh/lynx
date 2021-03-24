package me.ericfu.lightning.source.random;

import java.util.Random;

public interface RandomGenerator {

    Object generate(long i, Random rand);

}
