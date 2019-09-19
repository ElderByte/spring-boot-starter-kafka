package com.elderbyte.kafka.streams.builder;


import com.elderbyte.messaging.api.ElderMessage;

public class UpdateOrDelete<MK, U extends ElderMessage<MK>, D extends ElderMessage<MK>> {

    public static <MK, U extends ElderMessage<MK>, D extends ElderMessage<MK>> UpdateOrDelete<MK,U,D> update(U update){
        return new UpdateOrDelete<>(update, null);
    }
    public static <MK, U extends ElderMessage<MK>,D extends ElderMessage<MK>> UpdateOrDelete<MK,U,D> delete(D delete){
        return new UpdateOrDelete<>(null, delete);
    }

    private final U updated;
    private final D deleted;

    private UpdateOrDelete(U updated, D deleted) {
        if(updated == null && deleted == null) throw new IllegalStateException("You must either provide updated or deleted!");
        if(updated != null && deleted != null) throw new IllegalStateException("You must NOT provide updated and deleted!");
        this.updated = updated;
        this.deleted = deleted;
    }

    public boolean isDelete(){
        return deleted != null;
    }

    public U getUpdated() {
        return updated;
    }

    public D getDeleted() {
        return deleted;
    }

    public ElderMessage<MK> getMessage() {
        return isDelete() ? deleted : updated;
    }
}
