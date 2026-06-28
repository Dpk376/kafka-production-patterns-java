plugins {
    alias(libs.plugins.avro)
    `java-library`
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.kafka:spring-kafka")
    
    api("org.apache.avro:avro:1.11.3")
    api("io.confluent:kafka-avro-serializer:7.5.1")
}
