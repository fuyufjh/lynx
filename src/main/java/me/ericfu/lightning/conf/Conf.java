package me.ericfu.lightning.conf;

import me.ericfu.lightning.exception.InvalidConfigException;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for any configurations
 */
abstract class Conf {

    private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();

    /**
     * Do validations according to the constraints annotated
     *
     * @see javax.validation.constraints for all kinds of constraints
     */
    public final void validate() throws InvalidConfigException {
        Set<ConstraintViolation<Conf>> violations = factory.getValidator().validate(this);
        if (!violations.isEmpty()) {
            Kind kind = getClass().getAnnotation(Kind.class);
            String message = violations.stream()
                .map(v -> kind.value() + ": " + v.getPropertyPath() + " " + v.getMessage())
                .collect(Collectors.joining("; "));
            throw new InvalidConfigException(message);
        }
    }

}
