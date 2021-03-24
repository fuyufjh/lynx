package me.ericfu.lightning.conf;

import lombok.Data;
import me.ericfu.lightning.exception.InvalidConfigException;

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

    public GeneralConf getGeneral() {
        return general;
    }

    public void setGeneral(GeneralConf general) {
        this.general = general;
    }

    public SourceConf getSource() {
        return source;
    }

    public void setSource(SourceConf source) {
        this.source = source;
    }

    public SinkConf getSink() {
        return sink;
    }

    public void setSink(SinkConf sink) {
        this.sink = sink;
    }

    /**
     * Do validations according to the constraints annotated
     *
     * @see javax.validation.constraints for all kinds of constraints
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
