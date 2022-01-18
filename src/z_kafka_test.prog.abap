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
  consumer_uri        type string,
  destroyed_consumers type zkafka_destroyed_consumer_tab,
  dyc                 type line of zkafka_destroyed_consumer_tab.
data offpos type zkafka_offset_pos.
data offpos_tab type zkafka_offset_pos_tab.
data last_offset type int8.
field-symbols <tpc> type string.

data recs type zkafka_consumer_response_tab.

parameters:
  consumer type string default 'consu',
*  topic type string default 'flbookings',
  topic type string default 'comunidades',
  offset type int8 default 0.

translate topic to lower case.

CREATE OBJECT KAFKA_CON
  EXPORTING
*   HTTP_RFC_DEST =
    URL_BASE = 'http://esmadhp2vm03.mad.sap.corp:8082/'
*   URL_BASE = 'http://localhost:8082/'
*   URL_BASE = 'http://totoro.tritonas.net:8082/'
*   TOPIC    = 'haymazodepocos'
*   OFFSET   = '0'
  .


topics = kafka_con->get_topics(  ).
write:/ 'Kafka Topics:'. uline.

loop at topics assigning <tpc>.
  write:/ <tpc>.
endloop.
uline.
*
consumer_uri = kafka_con->create_consumer( consumer_name = consumer ).
kafka_con->subscribe_consumer_topics( consumer_name = consumer topic = topic ).
write:/ consumer_uri.

offpos-topic = topic.
offpos-partition = 0.
offpos-offset = offset.
append offpos to offpos_tab.

kafka_con->set_offset_position( exporting consumer_name = consumer kafka_offset_pos = offpos_tab ).
wait up to 3 seconds.


data respe type string.
*recs = kafka_con->consume( exporting consumer_name = consumer topic = topic value_data_type = 'BAPISBODAT' importing response_text = respe last_read_offset = last_offset ).
recs = kafka_con->consume( exporting consumer_name = consumer topic = topic value_data_type = 'ZSVH_COMUNIDADES' importing response_text = respe last_read_offset = last_offset ).
write:/ respe.
write:/ last_offset.
*
uline.

*recs = kafka_con->consume( exporting consumer_name = 'POCOS' topic = 'imanerd' max_bytes = 14000 value_data_type = 'BAPISBODAT'
*                           importing response_text = respe last_read_offset = last_offset ).
*write:/ respe.
*write:/ last_offset.
*
*uline.
*data rec2 type zkafka_consumer_response_tab.
*rec2 = kafka_con->consume( exporting consumer_name = 'POCOS' topic = 'imanerd' max_bytes = 30000 value_data_type = 'BAPISBODAT'
*                           importing response_text = respe last_read_offset = last_offset ).
*write:/ respe.
*write:/ last_offset.
*
*
*
*offpos-topic = 'imanerd'.
*offpos-partition = 0.
*offpos-offset = 24.
*append offpos to offpos_tab.
*
**kafka_con->set_offset_position( exporting consumer_name = 'POCOS' kafka_offset_pos = offpos_tab ).
*
**wait up to 3 seconds.
*
*uline.
*data rec3 type zkafka_consumer_response_tab.
*rec3 = kafka_con->consume( exporting consumer_name = 'POCOS' topic = 'imanerd' max_bytes = 1400 value_data_type = 'BAPISBODAT'
*                           importing response_text = respe last_read_offset = last_offset ).
*write:/ respe.
*write:/ last_offset.

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
