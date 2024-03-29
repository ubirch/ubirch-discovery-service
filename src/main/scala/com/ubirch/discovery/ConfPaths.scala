package com.ubirch.discovery

object ConfPaths {

  trait ConsumerConfPaths {
    final val CONSUMER_TOPICS = "kafkaApi.kafkaProducer.topic"
    final val CONSUMER_BOOTSTRAP_SERVERS = "kafkaApi.kafkaConsumer.bootstrapServers"
    final val CONSUMER_GROUP_ID = "kafkaApi.kafkaConsumer.groupId"
    final val CONSUMER_MAC_POOL_RECORDS = "kafkaApi.kafkaConsumer.maxPoolRecords"
    final val CONSUMER_GRACEFUL_TIMEOUT = "kafkaApi.kafkaConsumer.gracefulTimeout"
    final val CONSUMER_RECONNECT_BACKOFF_MS_CONFIG = "kafkaApi.kafkaConsumer.reconnectBackoffMsConfig"
    final val CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG = "kafkaApi.kafkaConsumer.reconnectBackoffMaxMsConfig"
  }

  trait ProducerConfPaths {
    final val PRODUCER_BOOTSTRAP_SERVERS = "kafkaApi.kafkaProducer.bootstrapServers"
    final val PRODUCER_ERROR_TOPIC = "kafkaApi.kafkaConsumer.errorTopic"
    final val PRODUCER_LINGER_MS = "kafkaApi.kafkaProducer.lingerMS"
  }

  trait DiscoveryConfPath {
    final val METRICS_SUBNAMESPACE = "kafkaApi.metrics.prometheus.namespace"
    final val GREMLIN_MAX_PARALLEL_CONN = "kafkaApi.gremlinConf.maxParallelConnection"
    final val BATCH_SIZE = "kafkaApi.batchSize"
    final val FLUSH = "flush"
    final val JG_HEALTH_CHECK_HASH_VALUE = "core.healthCheckHash"
  }

  trait RedisConfPaths {
    final val REDIS_PORT = "redis.port"
    final val REDIS_PASSWORD = "redis.password"
    final val REDIS_USE_REPLICATED = "redis.useReplicated"
    final val REDIS_CACHE_NAME = "redis.cacheName"
    final val REDIS_CACHE_TTL = "redis.ttl"
    final val REDIS_MAIN_HOST = "redis.mainHost"
    final val REDIS_REPLICATED_HOST = "redis.replicatedHost"
    final val REDIS_HOST = "redis.host"
  }

}
