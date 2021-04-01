package me.ericfu.lynx.model.conf;

import lombok.Data;
import me.ericfu.lynx.exception.InvalidConfigException;

import javax.validation.ConstraintViolation;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.NotNull;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public final class RootConf {

    @Valid
    @NotNull
    private GeneralConf general = new GeneralConf();

    @Valid
    @NotNull
    private SourceConf source;

    @Valid
    @NotNull
    private SinkConf sink;

    /**
     * Do validations according to the constraints annotated
     *
     * @see javax.validation.constraints all kinds of constraints
     */
    public final void validate() throws InvalidConfigException {
        final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Set<ConstraintViolation<RootConf>> violations = factory.getValidator().validate(this);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                .map(v -> v.getPropertyPath() + " " + v.getMessage())
                .collect(Collectors.joining("; "));
            throw new InvalidConfigException(message);
        }
    }
}
