package function;

/**
 * @author AaronY
 * @version 1.0
 * @since 2023/5/5
 */
public class UDF {

    public static final String EVAL_METHOD = "_eval";

    public String _eval(String email){
        System.out.println("got data ->" +  email);
        return email.split("@")[1];
    }
}
