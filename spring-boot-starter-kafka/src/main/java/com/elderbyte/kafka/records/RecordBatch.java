package com.elderbyte.kafka.records;

import com.elderbyte.kafka.consumer.processing.Processor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

import static java.util.stream.Collectors.toList;

public class RecordBatch<K, V> {

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    public static <K, V> RecordBatch<K, V> from(Collection<ConsumerRecord<K, V>> records){
        return new RecordBatch<>(records);
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final List<ConsumerRecord<K, V>> records;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new MessagesProcessor
     */
    protected RecordBatch(Collection<ConsumerRecord<K, V>> records) {
        this.records = new ArrayList<>(records);
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public List<ConsumerRecord<K, V>> getRecords(){
        return records;
    }

    public List<ConsumerRecord<K, V>> getDeletions() {
        return getRecords().stream()
                .filter(r -> r.value() == null)
                .collect(toList());
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Guarantees that the updated / deleted processor are called
     * at most once.
     *
     * Does not guarantee absolute order when invoking the processors.
     * However, does guarantee that only the latest record of a given key
     * is processed at all.
     *
     * Optimal for processing compacted topics.
     */
    public void compactedUpdateOrDelete(
            Processor<List<ConsumerRecord<K, V>>> updatedProcessor,
            Processor<List<ConsumerRecord<K, V>>> deletedProcessor
    ){
        try {
            var compacted = compact(records);

            var updated = new ArrayList<ConsumerRecord<K, V>>();
            var deleted = new ArrayList<ConsumerRecord<K, V>>();

            compacted.forEach(r -> {
                if(r.value() == null){
                    deleted.add(r);
                }else{
                    updated.add(r);
                }
            });

            deletedProcessor.proccess(deleted);
            updatedProcessor.proccess(updated);
        }catch (Exception e){
            throw new RuntimeException("Failed to handle record batch!", e);
        }
    }


    /**
     * Guarantees absolute order when processing the events.
     *
     * The update / deleted processors bucket messages until the opposite message
     * type is encountered, then the processor is invoked and the bucket of the opposite messages start.
     *
     * Optimizes the number of updated/deleted calls, but always guarantees the absolute message order.
     *
     * Use when replay of events must be in absolute order. Usually non compacted topics.
     *
     */
    public void linearUpdateOrDelete(Processor<List<ConsumerRecord<K, V>>> updatedProcessor,
                                     Processor<List<ConsumerRecord<K, V>>> deletedProcessor){

        var bucket = new ArrayList<ConsumerRecord<K, V>>();
        boolean delete = false;

        try {
            for (int i = 0; i < records.size(); i++) {
                var r = records.get(i);
                if(delete != (r.value() == null)){
                    if(!bucket.isEmpty()){
                        if(delete){
                            deletedProcessor.proccess(new ArrayList<>(bucket));
                        }else{
                            updatedProcessor.proccess(new ArrayList<>(bucket));
                        }
                        bucket.clear();
                    }
                    delete = !delete;
                }
                bucket.add(r);
            }

            // Finalize bucket if items remain in there
            if(!bucket.isEmpty()){
                if(delete){
                    deletedProcessor.proccess(new ArrayList<>(bucket));
                }else{
                    updatedProcessor.proccess(new ArrayList<>(bucket));
                }
                bucket.clear();
            }

        }catch (Exception e){
            throw new RuntimeException("", e);
        }
    }


    public List<ConsumerRecord<K, V>> compacted(){
        return compact(this.records);
    }

    @Override
    public String toString() {
        var deletions = getDeletions().size();
        return "RecordBatch{" +
                "records=" + records.size() +
                ", deletions=" + deletions +
                ", updates=" + (records.size() - deletions) +
                '}';
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private List<ConsumerRecord<K, V>> compact(List<ConsumerRecord<K, V>> records){
        var visitedKeys = new HashSet<K>();
        var compacted = new ArrayList<ConsumerRecord<K, V>>();
        for(int i=records.size()-1; i > -1; i--){
            var r = records.get(i);
            if(!visitedKeys.contains(r.key())){
                compacted.add(r);
                visitedKeys.add(r.key());
            }
        }
        Collections.reverse(compacted);
        return compacted;
    }
}
