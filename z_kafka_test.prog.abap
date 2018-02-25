*&---------------------------------------------------------------------*
*& Report  Z_KAFKA_TEST
*&
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*

REPORT  Z_KAFKA_TEST line-size 1023 no standard page heading.

data:
  http_status_code    type I,
  http_status_message type string,
  response_text       type string,
  wa_tnvp             type line of tihttpnvp,
  kafka_con           type ref to zcl_kafka_proxy,
  topics              type string_table,
  topic               type string,
  consumer_uri        type string,
  destroyed_consumers type zkafka_destroyed_consumer_tab,
  dyc                 type line of zkafka_destroyed_consumer_tab.


CREATE OBJECT KAFKA_CON
  EXPORTING
*   HTTP_RFC_DEST =
    URL_BASE = 'http://machost:8082/'
*    URL_BASE = 'http://localhost:8082/'
*   URL_BASE = 'http://totoro.tritonas.net:8082/'
*   TOPIC    = 'haymazodepocos'
*   OFFSET   = '0'
  .


topics = kafka_con->get_topics(  ).
write:/ 'Kafka Topics:'. uline.

loop at topics into topic.
  write:/ topic.
endloop.
uline.
*
*consumer_uri = kafka_con->create_consumer( consumer_name = 'POCOS' ).
*kafka_con->subscribe_consumer_topics( consumer_name = 'POCOS' topic = 'haymazodepocos' ).
*write:/ consumer_uri.
data respe type string.
*kafka_con->consume( exporting consumer_name = 'POCOS' importing response_text = respe ).
*write:/ respe.
*
uline.
data recs type zkafka_consumer_response_tab.
recs = kafka_con->consume( exporting consumer_name = 'KOIOIO' topic = 'imanerd' max_bytes = '1000000' value_data_type = 'BAPISBODAT' importing response_text = respe ).
write:/ respe.
**
**consumer_uri = kafka_con->create_consumer( consumer_name = 'consume_imanerd' ).
**write:/ consumer_uri.
**
**consumer_uri = kafka_con->create_consumer( consumer_name = 'consume_anothercoffee' ).
**write:/ consumer_uri.
*
**wait up to 4 seconds.
*
*uline.
destroyed_consumers = kafka_con->destroy_consumers( ).
*
loop at destroyed_consumers into dyc.
  write: / dyc-consumer_name, dyc-error_code, dyc-message.
endloop.
*
*
******
