package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created with IDEA
 * Author:catHome
 * Description: 反射工具类
 * Time:Create on 2018/8/13 10:25
 */
public class ReflectionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionUtil.class);

    /**
     * 创建实例
     */
    public static Object newInstance(Class<?> cls) {
        Object instance;
        try {
            instance = cls.newInstance();
        } catch (Exception e) {
            LOGGER.error("new instance failure", e);
            throw new RuntimeException(e);
        }
        return instance;
    }

    /**
     * 创建实例（根据类名）
     */
    public static Object newInstance(String className) {
        Class<?> cls = null;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return newInstance(cls);
    }

    /**
     * 调用方法
     */
    public static Object invokeMethod(Object obj, Method method, Object... args) {
        Object result;
        try {
            method.setAccessible(true);
            result = method.invoke(obj, args);
        } catch (Exception e) {
            LOGGER.error("invoke method failure", e);
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * 设置成员变量的值
     */
    public static void setField(Object obj, Field field, Object value) {
        try {
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            LOGGER.error("set field failure", e);
            throw new RuntimeException(e);
        }
    }

    public static Object getField(Object obj, Field field) {
        Object value = null;
        try {
            field.setAccessible(true);
            value = field.get(obj);
        } catch (Exception e) {
            LOGGER.error("set field failure", e);
            throw new RuntimeException(e);
        }
        return value;
    }

    public static Object getField(Object obj, String fieldName) {
        Object value = null;
        try {
            Field[] fields = obj.getClass().getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                String fieldNameTmp = field.getName();
                if (fieldName.equals(fieldNameTmp)) {
                    return getField(obj, field);
                }
            }
        } catch (Exception e) {
            LOGGER.error("get field by field name failure", e);
            throw new RuntimeException(e);
        }
        return value;
    }

    public static Object getFieldValue(Object obj, Field field, Class<?> clazz) {
        Object val = null;
        try {
            PropertyDescriptor pd = new PropertyDescriptor(field.getName(), clazz);
            Method rM = pd.getReadMethod();
            val = rM.invoke(obj);
        } catch (Exception e) {
            LOGGER.error("invoke get method failure", e);
        }
        return val;
    }

    public static Object getFieldValue(Object obj, String fieldName, Class<?> clazz) {
        Object val = null;
        try {
            PropertyDescriptor pd = new PropertyDescriptor(fieldName, clazz);
            Method rM = pd.getReadMethod();
            val = rM.invoke(obj);
        } catch (Exception e) {
            LOGGER.error("invoke get method failure", e);
        }
        return val;
    }

    public static void setFieldValue(Object obj, String fieldName, Object value) {
        try {
            PropertyDescriptor pd = new PropertyDescriptor(fieldName, obj.getClass());
            Method rM = pd.getWriteMethod();
            rM.invoke(obj, value);
        } catch (Exception e) {
            LOGGER.error("invoke get method failure", e);

        }
    }

}
