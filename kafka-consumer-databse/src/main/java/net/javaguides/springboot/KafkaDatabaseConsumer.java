package net.javaguides.springboot;

import net.javaguides.springboot.entity.WikimediaData;
import net.javaguides.springboot.repository.WikimediaDataRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    private WikimediaDataRepo dataRepo;

    public KafkaDatabaseConsumer(WikimediaDataRepo dataRepo) {
        this.dataRepo = dataRepo;
    }

    @KafkaListener(topics = "wikimedia_recentchange",
    groupId = "myGroup")
    public void consume(String eventMessage){
        LOGGER.info(String.format("Event Message Received -> %s", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);

        dataRepo.save(wikimediaData);
    }
}
