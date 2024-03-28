package ed.inf.adbs.lightdb.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CastList {
    /**
     * Casts each element of the given collection to the specified class and returns
     * a list of the cast elements.
     *
     * @param clazz The class to cast the elements of the collection to.
     * @param c     The collection whose elements are to be cast.
     * @return A list of the cast elements. The returned list is a new list and does
     *         not have any reference to the original collection.
     * @throws ClassCastException if any element of the collection cannot be cast to
     *                            the specified class.
     */
    public static <T> List<T> castList(Class<? extends T> clazz, Collection<?> c) {
        List<T> r = new ArrayList<T>(c.size());
        for (Object o : c)
            r.add(clazz.cast(o));
        return r;
    }
}
