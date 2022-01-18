class ZCL_HASTRAN_PROXY definition
  public
  final
  create public
  shared memory enabled .

public section.

  data FORM_FIELDS type TIHTTPNVP .
  constants XNL type ABAP_CHAR1 value %_NEWLINE ##NO_TEXT.
  constants XCRLF type ABAP_CR_LF value %_CR_LF ##NO_TEXT.
  data HTTP_RFCDEST type RFCDEST .
  data DEFAULT_TOPIC type STRING .
  data URL_BASE type STRING .
  data RQ type ref to DATA .
  data TOPICS type STRING_TABLE .
  data CONSUMERS type ZKAFKA_CONSUMER_INSTANCE_TAB .
  data CONSUMER_TOPICS type ZKAFKA_CONSUMER_TOPICS_TAB .
  data SLT_RQ type ref to DATA .
  data USER type STRING .
  data PASSWORD type STRING .
  data TOKEN type STRING .

  methods DELETE_RQ .
  methods CONSTRUCTOR
    importing
      !HTTP_RFC_DEST type RFCDEST optional
      !URL_BASE type STRING optional
      !USER type STRING optional
      !PASSWORD type STRING optional .
  class-methods HTTP_SEND
    importing
      value(POST_DATA) type STRING optional
      value(URL) type STRING optional
      value(METHOD) type STRING default 'GET'
      value(HTTP_RFC_DEST) type RFCDEST optional
      value(FORM_FIELDS) type TIHTTPNVP optional
      value(XPOST_DATA) type XSTRING optional
      value(CONTENT_TYPE) type STRING optional
      value(HEADER_FIELDS) type TIHTTPNVP optional
      value(ACCEPT_HEADER) type STRING optional
      !USER type STRING optional
      !PASSWORD type STRING optional
    exporting
      value(HTTP_STATUS_CODE) type I
      value(HTTP_STATUS_MESSAGE) type STRING
      value(RESPONSE_TEXT) type STRING
    exceptions
      SEND_ERROR
      RECEIVE_ERROR
      ERROR_CREATE_BY_URL
      ERROR_CREATE_BY_DEST
      PLEASE_SET_DESTINATION .
  class-methods ABAP2JSON
    importing
      !ABAP_DATA type DATA
      !NAME type STRING optional
      !UPCASE type XFELD optional
      !CAMELCASE type XFELD optional
      !ENCLOSED_IN_BRACES type XFELD optional
      !DELETE_NULLS type XFELD optional
    returning
      value(JSON_STRING) type STRING .
  methods GET_JSON_RESPONSE
    importing
      !JSON_RESPONSE_TEXT type STRING
    changing
      !RESP_DATA type ANY .
  methods ADD_SLT_RECORD
    importing
      !VALUE type ANY
      !PARTITION type INT4 optional
      !KEY type STRING optional
      !TABLENAME type STRING
      !OPERATION type STRING default 'I' .
  methods ADD_MESSAGE
    importing
      !VALUE type ANY
      !PARTITION type INT4 optional
      !KEY type STRING optional
      !EX_DATATYPE type STRING optional .
  methods SEND_ONE
    importing
      !STREAM type STRING optional
      !MESSAGE_STR type STRING optional
      !VALUE type ANY optional
      !EX_DATATYPE type STRING optional
    exporting
      !HTTP_STATUS_CODE type I
      !HTTP_STATUS_MESSAGE type STRING
      !RESPONSE_TEXT type STRING
    exceptions
      ERROR_IN_HTTP_SEND_CALL .
  class-methods JSON2ABAP
    importing
      !JSON_STRING type STRING optional
      !VAR_NAME type STRING optional
      !PROPERTY_PATH type STRING default 'json_obj'
      !DYN_FIELDS type TIHTTPNVP optional
    exporting
      !PROPERTY_TABLE type JS_PROPERTY_TAB
    changing
      !JS_OBJECT type ref to CL_JAVA_SCRIPT optional
      !ABAP_DATA type ANY optional
    raising
      ZCX_KFJSON .
  methods GET_TOKEN
    importing
      !USER type STRING optional
      !PASSWORD type STRING optional
      !PRIVILEGE type STRING
      !RESOURCE_TYPE type STRING
      !RESOURCE type STRING
    returning
      value(TOKEN) type STRING .
protected section.
*"* protected components of class ZCL_HASTRAN_PROXY
*"* do not include other source files here!!!
private section.
*"* private components of class ZCL_HASTRAN_PROXY
*"* do not include other source files here!!!
ENDCLASS.



CLASS ZCL_HASTRAN_PROXY IMPLEMENTATION.


method ABAP2JSON.
*/**********************************************/*
*/ This method takes any ABAP data variable and /*
*/ returns a string representing its value in   /*
*/ JSON format.                                 /*
*/ ABAP references are always de-referenced and /*
*/ treated as normal variables.                 /*
*/**********************************************/*

  type-pools: abap.

  constants:
    c_comma type c value ',',
    c_colon type c value ':',
    c_quote type c value '"'.

  data:
    dont_quote      type xfeld,
    json_fragments  type table of string,
    rec_json_string type string,
    l_type          type c,
    s_type          type c,
    l_comps         type i,
    l_lines         type i,
    l_index         type i,
    l_value         type string,
    l_name          type string,
    l_strudescr     type ref to cl_abap_structdescr.

  field-symbols:
    <abap_data> type any,
    <itab>      type any table,
    <stru>      type any table,
    <comp>      type any,
    <abapcomp>  type abap_compdescr.


  define get_scalar_value.
    " &1 : assigned var
    " &2 : abap data
    " &3 : abap type
    &1 = &2.
****************************************************
* Adapt some basic ABAP types (pending inclusion of all basic abap types?)
* Feel free to customize this for your needs
    case &3.
*       1. ABAP numeric types
      when 'I' or '4' or '8'. " Integer
        condense &1.
        if sign( &1 ) < 0.
          shift &1 by 1 places right circular.
        endif.
        dont_quote = 'X'.

      when 'F'. " Float
        condense &1.
        dont_quote = 'X'.

      when 'P'. " Packed number (used in quantities or currency, for example)
        condense &1.
        if sign( &1 ) < 0.
          shift &1 by 1 places right circular.
        endif.
        dont_quote = 'X'.

      when 'X'. " Hexadecimal
        condense &1.
        concatenate '0x' &1 into &1.
*        dont_quote = 'X'.
*        "Quote it, as JSON doesn't support Hex or Octal as native types.

*       2. ABAP char types
      when 'D'. " Date type
        CONCATENATE &1(4) '-' &1+4(2) '-' &1+6(2) INTO &1.

      when 'T'. " Time representation
        CONCATENATE &1(2) ':' &1+2(2) ':' &1+4(2) INTO &1.

      when 'N'. " Numeric text field
*           condense &1.

      when 'C' or 'g'. " Char sequences and Strings
* Put safe chars
        replace all occurrences of '\' in &1 with '\\' .
        replace all occurrences of '"' in &1 with '\"' .
        replace all occurrences of cl_abap_char_utilities=>cr_lf in &1 with '\r\n' .
        replace all occurrences of cl_abap_char_utilities=>newline in &1 with '\n' .
        replace all occurrences of cl_abap_char_utilities=>horizontal_tab in &1 with '\t' .
        replace all occurrences of cl_abap_char_utilities=>backspace in &1 with '\b' .
        replace all occurrences of cl_abap_char_utilities=>form_feed in &1 with '\f' .

      when 'y'.  " XSTRING
* Put the XSTRING in Base64
        &1 = cl_http_utility=>ENCODE_X_BASE64( &2 ).

      when others.
* Don't hesitate to add and modify scalar abap types to suit your taste.

    endcase.
** End of scalar data preparing.

* Enclose value in quotes (or not)
    if dont_quote ne 'X'.
      concatenate c_quote &1 c_quote into &1.
    endif.

    clear dont_quote.

  end-of-definition.

*********************
* Enclose in braces *
*********************
  if enclosed_in_braces is not initial.
    append '{' to json_fragments.
  endif.

***************************************************
*  Prepare field names, JSON does quote names!!   *
*  You must be strict in what you produce.        *
***************************************************
  if name is not initial.
    concatenate c_quote name c_quote c_colon into rec_json_string.
    append rec_json_string to json_fragments.
    clear rec_json_string.
  endif.

**
* Get ABAP data type
  describe field abap_data type l_type components l_comps.

***************************************************
*  Get rid of data references
***************************************************
  if l_type eq cl_abap_typedescr=>typekind_dref.
    assign abap_data->* to <abap_data>.
    if sy-subrc ne 0.
      append '{}' to json_fragments.
      concatenate lines of json_fragments into json_string.
      exit.
    endif.
  else.
    assign abap_data to <abap_data>.
  endif.

* Get ABAP data type again and start
  describe field <abap_data> type l_type components l_comps.

***************************************************
*  Tables
***************************************************
  if l_type eq cl_abap_typedescr=>typekind_table.
* '[' JSON table opening bracket
    append '[' to json_fragments.
    assign <abap_data> to <itab>.
    l_lines = lines( <itab> ).
    loop at <itab> assigning <comp>.
      add 1 to l_index.
*> Recursive call for each table row:
      rec_json_string = abap2json( abap_data = <comp> upcase = upcase camelcase = camelcase ).
      append rec_json_string to json_fragments.
      clear rec_json_string.
      if l_index < l_lines.
        append c_comma to json_fragments.
      endif.
    endloop.
    append ']' to json_fragments.
* ']' JSON table closing bracket


***************************************************
*  Structures
***************************************************
  else.
    if l_comps is not initial.
* '{' JSON object opening curly brace
      append '{' to json_fragments.
      l_strudescr ?= cl_abap_typedescr=>describe_by_data( <abap_data> ).
      loop at l_strudescr->components assigning <abapcomp>.
        l_index = sy-tabix .
        assign component <abapcomp>-name of structure <abap_data> to <comp>.
        l_name = <abapcomp>-name.
** ABAP names are usually in caps, set upcase to avoid the conversion to lower case.
        if upcase ne 'X'.
          " translate l_name to lower case.
          l_name = to_lower( l_name ).
        endif.
        if camelcase eq 'X'.
          l_name = to_mixed( val = l_name  case = 'a' ).
        endif.
        describe field <comp> type s_type.
        if s_type eq cl_abap_typedescr=>typekind_table or s_type eq cl_abap_typedescr=>typekind_dref or
           s_type eq cl_abap_typedescr=>typekind_struct1 or s_type eq cl_abap_typedescr=>typekind_struct2.
*> Recursive call for non-scalars:
          rec_json_string = abap2json( abap_data = <comp> name = l_name upcase = upcase camelcase = camelcase ).
        else.
          if s_type eq cl_abap_typedescr=>TYPEKIND_OREF or s_type eq cl_abap_typedescr=>TYPEKIND_IREF.
            rec_json_string = '"REF UNSUPPORTED"'.
          else.
            get_scalar_value rec_json_string <comp> s_type.
          endif.
          concatenate c_quote l_name c_quote c_colon rec_json_string into rec_json_string.
        endif.
          append rec_json_string to json_fragments.
        clear rec_json_string. clear l_name.
          if l_index < l_comps.
            append c_comma to json_fragments.
          endif.
      endloop.
      append '}' to json_fragments.
* '}' JSON object closing curly brace


****************************************************
*                  - Scalars -                     *
****************************************************
    else.
      get_scalar_value l_value <abap_data> l_type.
      append l_value to json_fragments.

    endif.
* End of structure/scalar IF block.
***********************************

  endif.
* End of main IF block.
**********************

*********************
* Enclose in braces *
*********************
  if enclosed_in_braces is not initial.
    append '}' to json_fragments.
  endif.

* Use a loop in older releases that don't support concatenate lines.
  concatenate lines of json_fragments into json_string.

endmethod.


  method ADD_MESSAGE.

    data datadesc type ref to CL_ABAP_TYPEDESCR.
    data datatype type string.
    data dref type ref to data.
    data record type zkafka_value_record.
    field-symbols <rq> type zkafka_value_record_tab.

    if value is not initial.
      if ex_datatype is not initial.
        datatype = ex_datatype.
      else.
        datadesc = cl_abap_typedescr=>DESCRIBE_BY_DATA( value ).
        datatype = datadesc->GET_RELATIVE_NAME( ).
      endif.
      create data dref type (datatype).
      get reference of value into dref.
      record-value = dref.
      record-key = key.
      record-partition = partition.
      assign me->rq->* to <rq>.
      append record to <rq>.
    endif.

  endmethod.


  method ADD_SLT_RECORD.

    data datadesc type ref to CL_ABAP_TYPEDESCR.
    data datatype type string.
    data dref type ref to data.
    data vref type ref to data.
    data record type zkafka_value_record.
    data slt_record type ZKAFKA_SLT_TABLE_RECORD.
    types: slt_record_tab type table of ZKAFKA_SLT_TABLE_RECORD.
    field-symbols:
      <rq>      type zkafka_value_record_tab,
      <slt_rq>  type slt_record_tab,
      <slt_rec> type ZKAFKA_SLT_TABLE_RECORD.

    slt_record-abap_srcsys = sy-sysid.
    slt_record-abap_tablename = tablename.
    case operation.
      when 'I'.
        slt_record-operation = 'insert'.
      when 'U'.
        slt_record-operation = 'update'.
      when 'D'.
        slt_record-operation = 'delete'.
    endcase.

    if value is not initial.
      if tablename is not initial.
        datatype = tablename.
      else.
        datadesc = cl_abap_typedescr=>DESCRIBE_BY_DATA( value ).
        datatype = datadesc->GET_RELATIVE_NAME( ).
      endif.
      create data vref type (datatype).
      get reference of value into vref.
      slt_record-payload = vref.
      assign me->slt_rq->* to <slt_rq>.
      append slt_record to <slt_rq>.

      read table <slt_rq> index sy-tabix assigning <slt_rec>.

      create data dref type ZKAFKA_SLT_TABLE_RECORD.
      get reference of <slt_rec> into dref.
      record-value = dref.
      record-key = key.
      record-partition = partition.
      assign me->rq->* to <rq>.
      append record to <rq>.
    endif.

  endmethod.


method CONSTRUCTOR.

  me->http_rfcdest = http_rfc_dest.
  me->url_base = url_base.
  me->user = user.
  me->password = password.

*  me->token = me->get_token(  ).





  data rqref type ref to data.
  create data rqref type zkafka_value_record_tab.
  me->rq = rqref.

  types: slt_record_tab type table of zkafka_slt_table_record.
  data sltrqref type ref to data.
  create data sltrqref type slt_record_tab.
  me->slt_rq = sltrqref.

endmethod.


  method DELETE_RQ.

    field-symbols <rq> type zkafka_value_record_tab.
    assign me->rq->* to <rq>.
    clear <rq>.

  endmethod.


method GET_JSON_RESPONSE.

  type-pools: abap, js.

  me->json2abap( exporting json_string = json_response_text  changing abap_data = resp_data ).
  exit.

endmethod.


  method GET_TOKEN.

    data auth_str type string.
    data basic_auth type string.

    data content_type type string.
    data method type string.
    data url_final type string.
    data pdata type string.
    data headers type TIHTTPNVP.
    data a_header type IHTTPNVP.

    data http_status_code type I.
    data http_status_message type string.
    data response_text type string.

    data my_user type string.
    data my_passwd type string.


    types: begin of token_info_type,
             privilege     type string,
             resource_type type string,
             resource      type string,
    end of token_info_type.

    data data_table type table of token_info_type.

    data token_info type token_info_type.


    data sws_token type sws_token_type.

    data sws_tab type table of sws_token_type.

    if USER is initial.
      my_user = me->user.
    else.
      my_user = USER.
    endif.

    if PASSWORD is initial.
      my_passwd = me->password.
    else.
      my_passwd = PASSWORD.
    endif.

*    concatenate my_user ':' my_passwd into auth_str.
*    basic_auth = cl_http_utility=>encode_base64( auth_str ).
*
*    a_header-name = 'Authorization'.
*    concatenate 'Basic' basic_auth into a_header-value separated by space.
*    append a_header to headers.
*    clear a_header.

    a_header-name = 'accept-encoding'.
    a_header-value = 'identity'.
    append a_header to headers.
    clear a_header.

    a_header-name = 'content-encoding'.
    a_header-value = 'identity'.
    append a_header to headers.
    clear a_header.


    token_info-privilege     = PRIVILEGE.
    token_info-resource      = RESOURCE.
    token_info-resource_type = RESOURCE_TYPE.



    method = 'POST'.
    content_type = 'text/plain'. " Shouldn't it be application/json????

    concatenate me->url_base '1/authorization' into url_final.
    condense url_final.

    append token_info to data_table.

    pdata = me->abap2json( abap_data = data_table camelcase = 'X' ).



    CALL METHOD ZCL_HASTRAN_PROXY=>HTTP_SEND
      EXPORTING
        METHOD                 = method
        URL                    = url_final
        CONTENT_TYPE           = content_type
*        ACCEPT_HEADER          = content_type
        HEADER_FIELDS          = headers
        POST_DATA              = pdata
        user                   = my_user
        password               = my_passwd
      IMPORTING
        HTTP_STATUS_CODE       = HTTP_STATUS_CODE
        HTTP_STATUS_MESSAGE    = HTTP_STATUS_MESSAGE
        RESPONSE_TEXT          = RESPONSE_TEXT
      EXCEPTIONS
        SEND_ERROR             = 1
        RECEIVE_ERROR          = 2
        ERROR_CREATE_BY_URL    = 3
        ERROR_CREATE_BY_DEST   = 4
        PLEASE_SET_DESTINATION = 5
        others                 = 6.
    IF SY-SUBRC <> 0.
*      raise ERROR_IN_HTTP_SEND_CALL.
    ENDIF.


    if http_status_code ne 200.

    else.

*     The response from the Str An in hana has the token name with a dash: 'sws-token'. This is *arguably* incorrect json
*     we may replace all dashes in the json keys with underscores '-' with '_' in json2abap line 181 if this happens a lot.
      replace first occurrence of 'sws-token' in response_text with 'sws_token'.

      me->json2abap( exporting json_string = response_text  changing abap_data = sws_tab ).

      read table sws_tab index 1 into sws_token.

      me->token = sws_token-sws_token.

      token = me->token.

    endif.






  endmethod.


method HTTP_SEND.

  data client type ref to if_http_client.
  data destination(255) type c.
  data errcode type sysubrc.
  data errmesg type string.

  if http_rfc_dest is not initial.

    destination = http_rfc_dest.

    CALL METHOD cl_http_client=>create_by_destination
      EXPORTING
        destination              = destination
      IMPORTING
        client                   = client
      EXCEPTIONS
        argument_not_found       = 1
        destination_not_found    = 2
        destination_no_authority = 3
        plugin_not_active        = 4
        internal_error           = 5
        others                   = 6.
    if sy-subrc <> 0.
      raise error_create_by_dest.
    endif.

  elseif url is not initial.

    CALL METHOD cl_http_client=>create_by_url
      EXPORTING
        url                = url
*       PROXY_HOST         =
*       PROXY_SERVICE      =
*       SSL_ID             =
*       SAP_USERNAME       =
*       SAP_CLIENT         =
      IMPORTING
        client             = client
      EXCEPTIONS
        argument_not_found = 1
        plugin_not_active  = 2
        internal_error     = 3
        others             = 4.
    if sy-subrc <> 0.
      raise error_create_by_url.
    endif.

  else.

    raise please_set_destination.

  endif.

  if user is not initial.
    client->request->set_authorization( USERNAME = user PASSWORD = password ).
  endif.

* client->request->set_header_field( name = '~request_method'  value = method ).  "Use this in older releases, like 6.20
  client->request->set_method( method ).

  if accept_header is not initial.
    client->request->set_header_field( name = 'Accept' value = ACCEPT_HEADER ).
  endif.

  if header_fields is not initial.
    client->request->set_header_fields( header_fields ).
  endif.

  if form_fields is not initial.
    client->request->set_form_fields( form_fields ).
  endif.

  if ( method EQ 'POST' or method  EQ 'PUT' )
       and ( post_data IS NOT INITIAL OR xpost_data is not initial ).
    if post_data is not initial.
      client->request->set_cdata( post_data ).
    elseif xpost_data is not initial.
      client->request->set_data( xpost_data ).
    endif.
  endif.

  if content_type is not initial.
    client->request->set_content_type( content_type ).
  endif.

  client->send( exceptions  HTTP_COMMUNICATION_FAILURE = 1
                            HTTP_INVALID_STATE         = 2
                            HTTP_PROCESSING_FAILED     = 3
                            HTTP_INVALID_TIMEOUT       = 4
                            others                     = 5 ).
  client->get_last_error( importing code = errcode message = errmesg ).

  if errcode ne 0.
    raise send_error.
  endif.

  client->receive( exceptions  HTTP_COMMUNICATION_FAILURE = 1
                               HTTP_INVALID_STATE         = 2
                               HTTP_PROCESSING_FAILED     = 3
                               others                     = 4 ).
  client->get_last_error( importing code = errcode message = errmesg ).

  if errcode ne 0.
    raise receive_error.
  endif.

  client->response->get_status( importing code = http_status_code reason = http_status_message ).
  response_text = client->response->get_cdata( ).

  call method client->close.

endmethod.


method JSON2ABAP.
*/************************************************/*
*/ Input any abap data and this method tries to   /*
*/ fill it with the data in the JSON string.      /*
*/  Thanks to Juan Diaz for helping here!!        /*
*/************************************************/*

  type-pools: abap, js.

  data:
    js_script         type string,
    js_started        type i value 0,
    l_json_string     type string,
    js_property_table type   js_property_tab,
    js_property       type line of js_property_tab,
    l_property_path   type string,
    compname          type string,
    item_path         type string.

  data:
    l_type   type c,
    l_value  type string,
    linetype type string,
    l_comp   type line of ABAP_COMPDESCR_TAB.

  data:
    datadesc type ref to CL_ABAP_TYPEDESCR,
    drefdesc type ref to CL_ABAP_TYPEDESCR,
    linedesc type ref to CL_ABAP_TYPEDESCR,
    strudesc type ref to CL_ABAP_STRUCTDESCR,
    tabldesc type ref to CL_ABAP_TABLEDESCR.

  data newline type ref to data.

  field-symbols:
    <abap_data> type any,
    <itab>      type any table,
    <comp>      type any,
    <jsprop>    type line of js_property_tab,
    <abapcomp>  type abap_compdescr.


  define assign_scalar_value.
    "   &1   <abap_data>
    "   &2   js_property-value
    describe field &1 type l_type.
    l_value = &2.
* convert or adapt scalar values to ABAP.
    case l_type.
      when 'D'. " date type
        if l_value cs '-'.
          replace all occurrences of '-' in l_value with space.
          condense l_value no-gaps.
        endif.
      when 'T'. " time type
        if l_value cs ':'.
          replace all occurrences of ':' in l_value with space.
          condense l_value no-gaps.
        endif.
      when others.
        " may be other conversions or checks could be implemented here.
    endcase.
    &1 = l_value.
  end-of-definition.


  if js_object is not bound.

    if json_string is initial. exit. endif. " exit method if there is nothing to parse

    l_json_string = json_string.
    " js_object = cl_java_script=>create( STACKSIZE = 16384 ).
    js_object = cl_java_script=>create( STACKSIZE = 16384 HEAPSIZE = 960000 ).

***************************************************
*  Parse JSON using JavaScript                    *
***************************************************
    js_object->bind( exporting name_obj = 'abap_data' name_prop = 'json_string'    changing data = l_json_string ).
    js_object->bind( exporting name_obj = 'abap_data' name_prop = 'script_started' changing data = js_started ).

* We use the JavaScript engine included in ABAP to read the JSON string.
* We simply use the recommended way to eval a JSON string as specified
* in RFC 4627 (http://www.ietf.org/rfc/rfc4627.txt).
*
* Security considerations:
*
*   Generally there are security issues with scripting languages.  JSON
*   is a subset of JavaScript, but it is a safe subset that excludes
*   assignment and invocation.
*
*   A JSON text can be safely passed into JavaScript's eval() function
*   (which compiles and executes a string) if all the characters not
*   enclosed in strings are in the set of characters that form JSON
*   tokens.  This can be quickly determined in JavaScript with two
*   regular expressions and calls to the test and replace methods.
*
*      var my_JSON_object = !(/[^,:{}\[\]0-9.\-+Eaeflnr-u \n\r\t]/.test(
*             text.replace(/"(\\.|[^"\\])*"/g, ''))) &&
*         eval('(' + text + ')');

    concatenate

         'var json_obj; '
         'var json_text; '

         'function start() { '
         '  if(abap_data.script_started) { return; } '
         '  json_text = abap_data.json_string;'
         '  json_obj = !(/[^,:{}\[\]0-9.\-+Eaeflnr-u \n\r\t]/.test( '
         '      json_text.replace(/"(\\.|[^"\\])*"/g, ''''))) && '
         '    eval(''('' + json_text + '')''); '
         '  abap_data.script_started = 1; '
         '} '

         'if(!abap_data.script_started) start(); '


       into js_script respecting blanks separated by xnl.

    js_object->compile( script_name = 'json_parser'     script = js_script ).
    js_object->execute( script_name = 'json_parser' ).

    if js_object->last_error_message is not initial.
      RAISE EXCEPTION type ZCX_KFJSON
        EXPORTING
          message = js_object->last_error_message.
    endif.

  endif.
** End of JS processing.

**
  if var_name is not initial.
    concatenate property_path var_name into l_property_path separated by '.'.
  else.
    l_property_path = property_path.
  endif.
**
**
  js_property_table = js_object->get_properties_scope_global( property_path = l_property_path ).
  property_table = js_property_table.

* Exit if abap_data is not supplied, normally when called
* from json_deserialize to get top level properties
  if abap_data is not supplied.
    exit.
  endif. "***

*
* Get ABAP data type, dereference if necessary and start
  datadesc = cl_abap_typedescr=>DESCRIBE_BY_DATA( abap_data ).
  if datadesc->kind eq cl_abap_typedescr=>kind_ref.
    assign abap_data->* to <abap_data>.
  else.
    assign abap_data to <abap_data>.
  endif.
  datadesc = cl_abap_typedescr=>DESCRIBE_BY_DATA( <abap_data> ).


  case datadesc->kind.

    when cl_abap_typedescr=>kind_elem.
* Scalar: process ABAP elements. Assume no type conversions for the moment.
      if var_name is initial.
        RAISE EXCEPTION type ZCX_KFJSON
          EXPORTING
            message = 'VAR_NAME is required for scalar values.'.
      endif.
      js_property_table = js_object->get_properties_scope_global( property_path = property_path ).
      read table js_property_table with key name = var_name into js_property.
      if sy-subrc eq 0.
        assign_scalar_value <abap_data> js_property-value.
      endif.


    when cl_abap_typedescr=>kind_struct.
* Process ABAP structures
      strudesc ?= datadesc.
      loop at js_property_table assigning <jsprop>.
        compname = <jsprop>-name.
        translate compname to upper case.
        read table strudesc->COMPONENTS with key name = compname into l_comp.
        if sy-subrc eq 0.
          assign component l_comp-name of structure <abap_data> to <comp>.
          case l_comp-type_kind.
            when    cl_abap_typedescr=>TYPEKIND_STRUCT1  " 'v'
                 or cl_abap_typedescr=>TYPEKIND_STRUCT2  " 'u'
                 or cl_abap_typedescr=>TYPEKIND_TABLE.   " 'h' (may need a different treatment one day)
              concatenate l_property_path <jsprop>-name into item_path separated by '.'.
*> Recursive call here
              json2abap( exporting property_path = item_path dyn_fields = dyn_fields changing abap_data = <comp> js_object = js_object ).

            when cl_abap_typedescr=>TYPEKIND_DREF. " 'l'

**** DO ALL THE WORK HERE!!
**** Assign dynamically abap data type.
**** Use a key-value table key=fieldname value=Abap_datatype!!!!!!!
              data dynf type ihttpnvp.
              read table dyn_fields with key name = compname into dynf.
              if sy-subrc eq 0.
                create data <comp> type (dynf-value).
                translate compname to lower case.
                concatenate l_property_path compname into item_path separated by '.'.
*> Recursive call here
                json2abap( exporting property_path = item_path dyn_fields = dyn_fields changing abap_data = <comp> js_object = js_object ).
*<<<<<<<<<<<<<<

              endif.

            when others.
* Process scalars in structures (same as the kind_elem above)
              assign_scalar_value <comp> <jsprop>-value.

          endcase.
        endif.
      endloop.

    when cl_abap_typedescr=>kind_table.
* Process ABAP tables
      if js_property_table is not initial.
        tabldesc ?= datadesc.
        linedesc = tabldesc->get_table_line_type( ).
        linetype = linedesc->get_relative_name( ).
        assign <abap_data> to <itab>.
        loop at js_property_table into js_property where name NE 'length'. " the JS object length
          create data newline type (linetype).
          assign newline->* to <comp>.
          case js_property-kind.
            when 'O'.
              concatenate l_property_path js_property-name into item_path separated by '.'.
              condense item_path.
*> Recursive call here
              json2abap( exporting property_path = item_path dyn_fields = dyn_fields changing abap_data = newline js_object = js_object ).
            when others. " Assume scalars, 'S', 'I', or other JS types
              " Process scalars in plain table components(same as the kind_elem above)
              assign_scalar_value <comp> js_property-value.
          endcase.
          insert <comp> into table <itab>.
          free newline.
        endloop.
      endif.

    when others. " kind_class, kind_intf
      " forget it.

  endcase.


endmethod.


  method SEND_ONE.

    data content_type type string.
    data l_http_rfc_dest type RFCDEST.
    data method type string.
    data pdata type string.
    data data_str type string.
    data url_final type string.
    data datadesc type ref to CL_ABAP_TYPEDESCR.
    data datatype type string.
    data dref type ref to data.
    data record_str type string.
    data token_auth type string.
        data headers type TIHTTPNVP.
    data a_header type IHTTPNVP.

    method = 'POST'.

    concatenate me->url_base '1/workspaces/default/projects/macabi/streams/EMPLOYEES' into url_final.
    condense url_final.

    if message_str is not initial.
      pdata = '"data":[]'.
    elseif value is not initial.
      if ex_datatype is not initial.
        datatype = ex_datatype.
      else.
        datadesc = cl_abap_typedescr=>DESCRIBE_BY_DATA( value ).
        datatype = datadesc->GET_RELATIVE_NAME( ).
      endif.
*
*      types: begin of han_data,
*          ESP_OPS type string,
*          include structure datatype,
*      end of han_data.
*


*      create data dref type (datatype).
*      get reference of value into dref.
      data_str = me->abap2json( abap_data = value enclosed_in_braces = ' ' ).
    endif.


    concatenate '{"commit":true,"data":[{"ESP_OPS":"i",'  data_str  '}]}' into pdata.

concatenate '"sws-token"="' me->token '"' into token_auth.

    a_header-name = 'Authorization'.
    concatenate 'SWS-Token' token_auth into a_header-value separated by space.
    append a_header to headers.
    clear a_header.

    a_header-name = 'accept-encoding'.
    a_header-value = 'identity'.
    append a_header to headers.
    clear a_header.

    a_header-name = 'content-encoding'.
    a_header-value = 'identity'.
    append a_header to headers.
    clear a_header.


    if me->http_rfcdest is not initial.
    endif.

    content_type = 'text/plain'.

    CALL METHOD ZCL_KAFKA_PROXY=>HTTP_SEND
      EXPORTING
        METHOD                 = method
        URL                    = url_final
*       FORM_FIELDS            = me->form_fields
        POST_DATA              = pdata
        CONTENT_TYPE           = content_type
      IMPORTING
        HTTP_STATUS_CODE       = HTTP_STATUS_CODE
        HTTP_STATUS_MESSAGE    = HTTP_STATUS_MESSAGE
        RESPONSE_TEXT          = RESPONSE_TEXT
      EXCEPTIONS
        SEND_ERROR             = 1
        RECEIVE_ERROR          = 2
        ERROR_CREATE_BY_URL    = 3
        ERROR_CREATE_BY_DEST   = 4
        PLEASE_SET_DESTINATION = 5
        others                 = 6.
    IF SY-SUBRC <> 0.
      raise ERROR_IN_HTTP_SEND_CALL.
    ENDIF.


    if http_status_code ne 200.

    endif.

  endmethod.
ENDCLASS.
