package edu.anadolu.helper;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Emre
 * Date: 13.07.2012
 * Time: 17:49
 * To change this template use File | Settings | File Templates.
 */
public class JsonHelper {
    private static final Logger logger = Logger.getLogger(JsonHelper.class);
    public static String convertObjectToJson(Object o){
        StringBuilder jsonBuilder = new StringBuilder();
        try {
            jsonBuilder.append(PojoMapper.toJson(o,true));
        } catch (IOException e) {
            logger.error(e);
        }
        return jsonBuilder.toString().replace("\n", "");
    }
}
