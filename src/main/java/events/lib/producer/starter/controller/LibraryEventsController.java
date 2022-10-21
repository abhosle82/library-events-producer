package events.lib.producer.starter.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import events.lib.producer.starter.domain.LibraryEvent;
import events.lib.producer.starter.domain.LibraryEventType;
import events.lib.producer.starter.producer.LibraryEventsProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class LibraryEventsController {

    @Autowired
    private LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryevents")
    public ResponseEntity<LibraryEvent> addLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventsProducer.sendLibraryEventWithHeaders(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevents")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        if(libraryEvent.getLibraryEventId()==null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please provide the library event id");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventsProducer.sendLibraryEventWithHeaders(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
