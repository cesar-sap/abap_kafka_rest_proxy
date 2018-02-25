*&---------------------------------------------------------------------*
*& Report  Z_KAFKA_SEND_BOOKINGS
*&
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*

REPORT  Z_KAFKA_SEND_BOOKINGS line-size 1023 no standard page heading.

data:
  http_status_code type I,
  http_status_message type string,
  response_text type string,
  wa_tnvp type line of tihttpnvp,
  kafka_con type ref to zcl_kafka_proxy,
  t_bookings type table of BAPISBODAT,
  msgidx type i,
  idxstr type string,
  nlines type i.


*data wa_booking type BAPISBODAT.
field-symbols <fs_booking> type BAPISBODAT.

parameters:
  airline like BAPISBOKEY-AIRLINEID,
  t_agency like BAPISBODAT-AGENCYNUM,
  c_number like BAPISCUKEY-CUSTOMERID,
  max_rows like BAPISFLAUX-BAPIMAXROW.

select-options fl_date for <fs_booking>-flightdate.
select-options bk_date for <fs_booking>-bookdate.


CALL FUNCTION 'BAPI_FLBOOKING_GETLIST'
  EXPORTING
    AIRLINE            = airline
    TRAVEL_AGENCY      = t_agency
    CUSTOMER_NUMBER    = c_number
    MAX_ROWS           = max_rows
  TABLES
    FLIGHT_DATE_RANGE  = fl_date
    BOOKING_DATE_RANGE = bk_date
*   EXTENSION_IN       =
    BOOKING_LIST       = t_bookings
*   EXTENSION_OUT      =
*   RETURN             =
  .

*break-point id z_dcn.

CREATE OBJECT KAFKA_CON
  EXPORTING
*    HTTP_RFC_DEST =
    URL_BASE = 'http://machost:8082/'
*    TOPIC = 'haymazodepocos'
*    OFFSET = '0'
   .

loop at t_bookings assigning <fs_booking>.
  add 1 to msgidx.
  idxstr = msgidx.
  kafka_con->produce_one( topic = 'imanerd' value = <fs_booking> ).
endloop.

write: idxstr.

*****
