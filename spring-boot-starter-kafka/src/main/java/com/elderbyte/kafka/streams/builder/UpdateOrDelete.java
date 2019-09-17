package com.elderbyte.kafka.streams.builder;

public class UpdateOrDelete<U, D> {

    public static <U,D> UpdateOrDelete<U,D> update(U update){
        return new UpdateOrDelete<>(update, null);
    }
    public static <U,D> UpdateOrDelete<U,D> delete(D delete){
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

    public Object getMessage() {
        return isDelete() ? deleted : updated;
    }
}
