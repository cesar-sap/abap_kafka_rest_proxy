class ZCX_KFJSON definition
  public
  inheriting from CX_STATIC_CHECK
  final
  create public .

public section.

  constants ZCX_KFJSON type SOTR_CONC value '000C293CED061EE2A2D7BA5F40F0C8DE' ##NO_TEXT.
  data MESSAGE type STRING value 'undefined' ##NO_TEXT.

  methods CONSTRUCTOR
    importing
      !TEXTID like TEXTID optional
      !PREVIOUS like PREVIOUS optional
      !MESSAGE type STRING default 'undefined' .
protected section.
private section.
ENDCLASS.



CLASS ZCX_KFJSON IMPLEMENTATION.


  method CONSTRUCTOR.
CALL METHOD SUPER->CONSTRUCTOR
EXPORTING
TEXTID = TEXTID
PREVIOUS = PREVIOUS
.
 IF textid IS INITIAL.
   me->textid = ZCX_KFJSON .
 ENDIF.
me->MESSAGE = MESSAGE .
  endmethod.
ENDCLASS.
