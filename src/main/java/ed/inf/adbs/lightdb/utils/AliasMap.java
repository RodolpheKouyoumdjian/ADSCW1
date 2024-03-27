package ed.inf.adbs.lightdb.utils;

import java.util.HashMap;
import java.util.Map;

// The AliasMap class is used to manage aliases in SQL queries.
// It keeps a mapping of alias names to actual table names.
public class AliasMap {
    // A map to store the alias and its corresponding actual table name.
    private static final Map<String, String> aliasMap = new HashMap<>();

    // Returns the alias map.
    public static Map<String, String> getAliasmap() {
        return aliasMap;
    }

    // Adds a new alias to the map.
    // @param alias The alias name.
    // @param actualName The actual table name.
    public static void addAlias(String alias, String actualName) {
        aliasMap.put(alias, actualName);
    }

    // Resolves an alias to its actual table name.
    // If the name is not an alias, it returns the name itself.
    // @param name The alias or table name to resolve.
    public static String resolveAlias(String name) {
        return aliasMap.getOrDefault(name, name);
    }
}