package function;

import java.util.Arrays;
import java.util.List;

/**
 * @author AaronY
 * @version 1.0
 * @since 2023/5/5
 */
public class UDTF {

    public static final String EVAL_METHOD = "_eval";

    public List<String> _eval(String email){
        return Arrays.asList(email.split("@"));
    }
}
