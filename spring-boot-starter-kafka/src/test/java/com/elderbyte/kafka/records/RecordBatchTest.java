package com.elderbyte.kafka.records;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class RecordBatchTest {


    @Test
    public void compacted() {
        var batch = new RecordBatch<>(Arrays.asList(
                record("A", "1"),
                record("B", "1"),
                record("C", "1"),
                record("D", "1"),
                record("A", "2"),
                record("B", null)
        ));

        var compacted = batch.compacted();

        var c1 = compacted.get(0);
        var d1 = compacted.get(1);
        var a2 = compacted.get(2);
        var btomb = compacted.get(3);

        assertEquals("C", c1.key());
        assertEquals("D", d1.key());
        assertEquals("A", a2.key());
        assertEquals("2", a2.value());
        assertEquals("B", btomb.key());
        assertNull(btomb.value());
    }

    @Test
    public void compactedUpdateOrDelete() {

        var batch = new RecordBatch<>(Arrays.asList(
                record("A", "1"),
                record("B", "1"),
                record("C", "1"),
                record("D", "1"),
                record("A", "2"),
                record("B", null)
        ));

        var records = new ArrayList<List<ConsumerRecord<String, String>>>();
        batch.compactedUpdateOrDelete(
                records::add,
                records::add
                );

        var deleted = records.get(0);
        var updated = records.get(1);

        assertEquals("B", deleted.get(0).key());
        assertEquals(1, deleted.size());

        assertEquals("C", updated.get(0).key());
        assertEquals("D", updated.get(1).key());
        assertEquals("A", updated.get(2).key());

        assertEquals(3, updated.size());
    }

    @Test
    public void linearUpdateOrDelete() {

        var batch = new RecordBatch<>(Arrays.asList(
                record("A", "1"),
                record("B", "1"),
                record("C", "1"),
                record("D", "1"),
                record("A", "2"),
                record("B", null),
                record("C", null),
                record("A", "3")
        ));

        var records = new ArrayList<List<ConsumerRecord<String, String>>>();
        batch.linearUpdateOrDelete(
                records::add,
                records::add
        );

        var updates = records.get(0);
        var delete = records.get(1);
        var updates2 = records.get(2);
        assertEquals(3, records.size());

        var a1 = updates.get(0);
        var b1 = updates.get(1);
        var c1 = updates.get(2);
        var d1 = updates.get(3);
        var a2 = updates.get(4);

        var bN = delete.get(0);
        var cN = delete.get(1);

        var a3 = updates2.get(0);


        assertEquals("A", a1.key());
        assertEquals("B", b1.key());
        assertEquals("C", c1.key());
        assertEquals("D", d1.key());
        assertEquals("A", a2.key());
        assertEquals("2", a2.value());

        assertEquals("B", bN.key());
        assertEquals(null, bN.value());
        assertEquals("C", cN.key());
        assertEquals(null, cN.value());

        assertEquals("A", a3.key());

    }

    @Test
    public void linearUpdateOrDelete_2() {

        var batch = new RecordBatch<>(Arrays.asList(
                record("A", "1"),
                record("B", null),
                record("C", "1")
        ));

        var records = new ArrayList<List<ConsumerRecord<String, String>>>();
        batch.linearUpdateOrDelete(
                records::add,
                records::add
        );

        var a = records.get(0);
        var b = records.get(1);
        var c = records.get(2);

        assertEquals("A", a.get(0).key());
        assertEquals(1, a.size());

        assertEquals("B", b.get(0).key());
        assertEquals(null, b.get(0).value());
        assertEquals(1, b.size());

        assertEquals("C", c.get(0).key());
        assertEquals(1, c.size());
    }



    private ConsumerRecord<String, String> record(String key, String value){
        return new ConsumerRecord<>("top", 0, 0, key, value);
    }
}
