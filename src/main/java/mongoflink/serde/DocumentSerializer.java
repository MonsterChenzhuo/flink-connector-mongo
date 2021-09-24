package mongoflink.serde;

import org.bson.Document;

import java.io.Serializable;

/**
 * @author chenzhuoyu
 * @date 2021/9/19 23:22
 * DocumentSerializer serialize POJOs or other Java objects into {@link Document}.
 **/
public interface DocumentSerializer<T> extends Serializable {

    /**
     * Serialize input Java objects into {@link Document}.
     * @param object The input object.
     * @return The serialized {@link Document}.
     */
    Document serialize(T object);
}
