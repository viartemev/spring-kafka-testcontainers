package com.example.demo

import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaListener
import java.util.*


@EnableKafka
@Configuration
class KafkaConfig {

    val invocations: MutableList<String> = ArrayList()

    @KafkaListener(topics = ["test-topic-1"], groupId = "test-group")
    fun onKafkaEvent(message: String) {
        println("There is a message")
        invocations.add(message)
    }

}