package events.lib.producer.starter.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import events.lib.producer.starter.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

@Component
@Slf4j
public class LibraryEventsProducer {

    private KafkaTemplate<Integer,String> kafkaTemplate;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Autowired
    private ObjectMapper mapper;

    private String topic="library-events";
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = mapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
                 @Override
                 public void onFailure(Throwable ex) {
                     handleFailure(key,value,ex);
                 }

                 @Override
                 public void onSuccess(SendResult<Integer, String> result) {
                    handdleSuccess(key,value,result);
                 }
             }
        );

    }
    public void sendLibraryEventWithHeaders(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = mapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key,value,"library-events");
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
                                         @Override
                                         public void onFailure(Throwable ex) {
                                             handleFailure(key,value,ex);
                                         }

                                         @Override
                                         public void onSuccess(SendResult<Integer, String> result) {
                                             handdleSuccess(key,value,result);
                                         }
                                     }
        );

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeader = List.of(new RecordHeader("event-source","scanner".getBytes()));
        return new ProducerRecord<>(topic,null,key,value,recordHeader);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.info("Unable to publish the message with key {}, value {}",key,value);
        try{
            throw ex;
        }catch(Throwable tex){
            log.info("Unable to publish the message with key {}, value {} due to exception {}",key,value,ex.getMessage());
        }
    }

    private void handdleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Publishing the message with key {}, value {} on partition {}",key,value,result.getRecordMetadata().partition());
    }
}
