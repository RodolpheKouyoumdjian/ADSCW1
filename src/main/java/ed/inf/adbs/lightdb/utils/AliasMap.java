package ed.inf.adbs.lightdb.utils;

import java.util.HashMap;
import java.util.Map;

public class AliasMap {
    private static final Map<String, String> aliasMap = new HashMap<>();

    public static Map<String, String> getAliasmap() {
        return aliasMap;
    }

    public static void addAlias(String alias, String actualName) {
        aliasMap.put(alias, actualName);
    }

    public static String resolveAlias(String name) {
        return aliasMap.getOrDefault(name, name);
    }
}