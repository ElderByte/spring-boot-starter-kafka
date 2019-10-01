package com.elderbyte.kafka.messages;

import com.elderbyte.commons.utils.NumberUtil;

import java.lang.reflect.Field;

public class ReflectionSupport {

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public static  <T> T createInstance(Class<T> clazz){
        try {
            var constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (Exception e) {
            throw new InvalidMessageException("Failed to access constructor / create new instance of "+clazz.getName(), e);
        }
    }


    @SuppressWarnings("unchecked")
    public static  <V> void setFieldString(Field field, V message, String headerValue){
        if(field.getType() == String.class){
            setField(field, message, headerValue);
        }else if(NumberUtil.isNumeric(field.getType())) {
            var number = NumberUtil.parseNumber(headerValue,  (Class<Number>)field.getType());
            setField(field, message, number);
        }else{
            throw new InvalidMessageException("Field " +
                    " "+field.getName()+" was of unsupported type "+field.getType()+" ! ");
        }
    }

    public static  <V> void setField(Field field, V message, Object value){
        try {
            field.set(message, value);
        } catch (IllegalAccessException e) {
            throw new InvalidMessageException("Failed to access value of key field: "+field.getName(), e);
        }
    }


    public static <V> Object getField(Field field, V message){
        try {
            return field.get(message);
        } catch (IllegalAccessException e) {
            throw new InvalidMessageException("Failed to access value of field: "+field.getName(), e);
        }
    }

    public static <V> String getFieldAsString(Field field, V message){
        var value = getField(field, message);
        if(value != null){
            return value.toString();
        }
        return null;
    }

    public static <V> String getRequiredFieldAsString(Field field, V message){
        var val = getFieldAsString(field, message);
        if(val == null){
            throw new InvalidMessageException("The field "+field.getName()+" of the message must not be null!");
        }
        return val;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
