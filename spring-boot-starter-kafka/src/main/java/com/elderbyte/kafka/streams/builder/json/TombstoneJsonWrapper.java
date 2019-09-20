package com.elderbyte.kafka.streams.builder.json;

import com.elderbyte.kafka.streams.builder.UpdateOrDelete;
import com.elderbyte.messaging.api.ElderMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.lang.Nullable;

import java.util.Optional;

public class TombstoneJsonWrapper<V> {

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    public static
            <
            MK,
            U extends ElderMessage<MK>,
            D extends ElderMessage<MK>
            >
    TombstoneJsonWrapper<U> from(ObjectMapper mapper, UpdateOrDelete<MK, U, D> updateOrDelete){

        TombstoneJsonWrapper<U> tombstoneWrapper;
        if(updateOrDelete.isDelete()){
            tombstoneWrapper = TombstoneJsonWrapper.tombstone();
        }else{
            tombstoneWrapper = TombstoneJsonWrapper.ofNullable(mapper, updateOrDelete.getUpdated());
        }
        return tombstoneWrapper;
    }

    public static <T> TombstoneJsonWrapper<T> tombstone(){
        return new TombstoneJsonWrapper<>(
                null
        );
    }

    public static <T> TombstoneJsonWrapper<T> ofNullable(ObjectMapper mapper, @Nullable T value){
        return new TombstoneJsonWrapper<>(
                value == null ? null : mapper.valueToTree(value)
        );
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    public JsonNode value;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    public TombstoneJsonWrapper(){}

    public TombstoneJsonWrapper(@Nullable JsonNode value){
        this.value = value;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/


    public Optional<V> getValue(ObjectMapper mapper, Class<V> clazz){
        return Optional.ofNullable(value)
                        .flatMap(v -> {
                            if(value.isNull() || value.isMissingNode()){
                                return Optional.empty();
                            }else{
                                return Optional.of(mapper.convertValue(value, clazz));
                            }
                        });
    }
}
