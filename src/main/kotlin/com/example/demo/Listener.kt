package com.example.demo

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.*


@Component
class Listener {
    val invocations: MutableList<String> = ArrayList()

    @KafkaListener(topics = ["test-topic-1"], groupId = "test-group")
    fun onKafkaEvent(message: String) {
        println("There is a message")
        invocations.add(message)
    }
}