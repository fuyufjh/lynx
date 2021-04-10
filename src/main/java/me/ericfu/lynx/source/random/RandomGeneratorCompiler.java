package me.ericfu.lynx.source.random;

import me.ericfu.lynx.exception.DataSourceException;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compiler to compile user-specified code into a RandomGenerator
 */
class RandomGeneratorCompiler {

    private static final Logger logger = LoggerFactory.getLogger(RandomGenerator.class);

    private static final String EXPR_TEMPLATE =
        "public ${RETURN_TYPE} generate(long rownum, java.util.Random rand) { return ${EXPRESSION}; }";

    private static final String RETURN_TYPE = "${RETURN_TYPE}";
    private static final String EXPRESSION = "${EXPRESSION}";

    private static final String CODE_BLOCK_TEMPLATE =
        "public ${RETURN_TYPE} generate(long rownum, java.util.Random rand) ${CODE_BLOCK}";

    private static final String CODE_BLOCK = "${CODE_BLOCK}";

    private static final String CLASS_NAME_TEMPLATE =
        "me.ericfu.lynx.source.random.custom.CustomGenerator%06d";

    private static final AtomicInteger counter = new AtomicInteger();

    public RandomGenerator compile(String code, Class<?> returnType) throws DataSourceException {
        ClassBodyEvaluator evaluator = new ClassBodyEvaluator();
        evaluator.setImplementedInterfaces(new Class[]{RandomGenerator.class});
        evaluator.setClassName(String.format(CLASS_NAME_TEMPLATE, counter.getAndIncrement()));

        String classBody;
        if (code.startsWith("{") && code.endsWith("}")) {
            // Generate class from a code block
            classBody = CODE_BLOCK_TEMPLATE.replace(CODE_BLOCK, code).replace(RETURN_TYPE, returnType.getName());
        } else {
            // Generate class from an expression
            classBody = EXPR_TEMPLATE.replace(EXPRESSION, code).replace(RETURN_TYPE, returnType.getName());
        }

        logger.debug("Class body of '{}':\n{}", code, classBody);

        try {
            evaluator.cook(classBody);
        } catch (CompileException e) {
            throw new DataSourceException("cannot compile random expression: " + code +
                ", class body is:\n" + classBody, e);
        }

        try {
            return (RandomGenerator) evaluator.getClazz().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AssertionError(); // unreachable
        }
    }
}
