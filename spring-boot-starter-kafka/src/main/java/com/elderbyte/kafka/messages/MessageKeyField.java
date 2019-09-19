package com.elderbyte.kafka.messages;

import java.lang.reflect.Field;

class MessageKeyField {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Field field;
    private final boolean populateField;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    public MessageKeyField(Field field, boolean populateField) {
        this.field = field;
        this.populateField = populateField;
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public Field getField() {
        return field;
    }

    public boolean isPopulateField() {
        return populateField;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
