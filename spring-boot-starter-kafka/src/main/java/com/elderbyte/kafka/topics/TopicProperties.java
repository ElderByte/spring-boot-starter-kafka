package com.elderbyte.kafka.topics;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TopicProperties {

        /***************************************************************************
         *                                                                         *
         *  Fields                                                                 *
         *                                                                         *
         **************************************************************************/

        private String name;
        private int partitions = 1;
        private short replicas = 1;

        private String cleanUpPolicy = "delete"; // "compact"
        private Duration retention = null;

        //private Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        private Map<String, String> properties = new HashMap<>();

        private boolean create = true;

        /***************************************************************************
         *                                                                         *
         *  Properties                                                             *
         *                                                                         *
         **************************************************************************/

        public String getName() {
                return name;
        }

        public void setName(String name) {
                this.name = name;
        }

        public int getPartitions() {
                return partitions;
        }

        public void setPartitions(int partitions) {
                this.partitions = partitions;
        }

        public short getReplicas() {
                return replicas;
        }

        public void setReplicas(short replicas) {
                this.replicas = replicas;
        }

        public Map<String, String> getProperties() {
                return properties;
        }

        public void setProperties(Map<String, String> properties) {
                this.properties = properties;
        }

        public String getCleanUpPolicy() {
                return cleanUpPolicy;
        }

        public void setCleanUpPolicy(String cleanUpPolicy) {
                this.cleanUpPolicy = cleanUpPolicy;
        }

        public Duration getRetention() {
                return retention;
        }

        public void setRetention(Duration retention) {
                this.retention = retention;
        }

        public boolean isCreate() {
                return create;
        }

        public void setCreate(boolean create) {
                this.create = create;
        }
}
