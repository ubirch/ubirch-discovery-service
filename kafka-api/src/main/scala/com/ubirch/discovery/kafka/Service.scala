package com.ubirch.discovery.kafka

import com.ubirch.discovery.kafka.consumer.StringConsumer


object Service extends Boot {

  def main(args: Array[String]): Unit = {

    val consumer = StringConsumer


    consumer.start()

    while(true) {

    }

  }

}