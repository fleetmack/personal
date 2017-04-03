
  CREATE TABLE "EDWMGR"."ZDT_DEDUCTION" 
   (	"DEDUCTION_KEY" NUMBER NOT NULL ENABLE, 
	"MULTI_SOURCE" VARCHAR2(5 BYTE), 
	"CARRIER" VARCHAR2(63 BYTE), 
	"CARRIER_SHORT_DESC" VARCHAR2(255 BYTE), 
	"CARRIER_LONG_DESC" VARCHAR2(255 BYTE), 
	"COVERAGE_OPTION" VARCHAR2(255 BYTE), 
	"COVERAGE_OPTION_DESC" VARCHAR2(255 BYTE), 
	"DEDN_TYPE" VARCHAR2(63 BYTE), 
	"SYSTEM_LOAD_PROCESS" VARCHAR2(30 CHAR) NOT NULL ENABLE, 
	"SYSTEM_LOAD_TMSTMP" DATE DEFAULT sysdate NOT NULL ENABLE, 
	 CONSTRAINT "PK_ZDT_DEDUCTION" PRIMARY KEY ("DEDUCTION_KEY")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "EDW_TBS"  ENABLE
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "EDW_TBS" ;

  CREATE UNIQUE INDEX "EDWMGR"."I01_ZDT_DEDUCTION" ON "EDWMGR"."ZDT_DEDUCTION" ("MULTI_SOURCE", "CARRIER", "COVERAGE_OPTION", "DEDN_TYPE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "INDX" ;

  CREATE OR REPLACE TRIGGER "EDWMGR"."ZDT_DEDUCTION_SEQ" BEFORE INSERT ON "EDWMGR"."ZDT_DEDUCTION" FOR EACH ROW 
BEGIN
    IF :new.DEDUCTION_KEY IS NULL THEN
        SELECT ZDS_DEDUCTION_SEQ.nextval INTO :new.DEDUCTION_KEY FROM DUAL;
    END IF;
END;
/
ALTER TRIGGER "EDWMGR"."ZDT_DEDUCTION_SEQ" ENABLE;