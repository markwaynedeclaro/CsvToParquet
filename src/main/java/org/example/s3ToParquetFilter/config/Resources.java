package org.example.s3ToParquetFilter.config;

import lombok.extern.log4j.Log4j2;

import java.util.Properties;
import java.util.ResourceBundle;

/**
 * Sets the resource
 */
@Log4j2
public class Resources {

    private static String SPRING_PROFILES_ACTIVE = "spring.profiles.active";
    public static Resources MAIN_RB = new Resources();
    private Properties properties;

    public Resources() {
        properties = new Properties();
        ResourceBundle rb = ResourceBundle.getBundle("application");
        for(String key : rb.keySet()) {
            properties.put(key, rb.getString(key));
        }

        // read profile properties: e.g.: application-config.properties
        String profile = System.getProperty(SPRING_PROFILES_ACTIVE);
        if (profile == null || profile.length() == 0) {
            profile = properties.getProperty(SPRING_PROFILES_ACTIVE);
        }
        if (profile != null && profile.length() > 0) {
            rb = ResourceBundle.getBundle("application-" + profile);
            if (rb != null) {
                for (String key : rb.keySet()) {
                    properties.put(key, rb.getString(key));
                }
            }
        }

    }

    public String get(String property) {
        return properties.getProperty(property).trim();
    }

}
