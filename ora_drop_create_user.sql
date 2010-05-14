
--
-- SQL Plus commands 
--
SET LINESIZE 150;
SET SERVEROUTPUT on SIZE 1000000 FORMAT WRAPPED;
SET FEEDBACK OFF
SET VERIFY OFF

-- get the latest timestamp
column dcol new_value currTS noprint
select to_char(systimestamp, 'HH24:MI:SS.ff') dcol from dual;

-- Check to see if the user exists and then drop the user
prompt &currTS  Verifying user existence and started DROP process for -->  &1 ;
WHENEVER SQLERROR EXIT SQL.SQLCODE
declare
  i integer;
begin
  begin -- Exception Block
  --dbms_output.put_line( '&currTS  About to drop the user -->  &1' );
  for x in (select t.username from all_users t where t.username = '&1') loop
      execute immediate 'DROP USER &1 CASCADE';
  end loop; 
  exception
        when others then
            -- dbms_output.put_line('&currTS  ORA Error: '|| SQLCODE ||': ' ||substr(SQLERRM, 1, 100) );
	     raise; 
  end;   
end;
/

select to_char(systimestamp, 'HH24:MI:SS.ff') dcol from dual;
prompt &currTS  Dropped user [OK] ;

-- Recreate the user
select to_char(systimestamp, 'HH24:MI:SS.ff') dcol from dual;
prompt &currTS  Recreating the user    -->  &1 ;

-- Create the user once more
WHENEVER SQLERROR EXIT SQL.SQLCODE 
CREATE USER &1
  IDENTIFIED BY &2
  DEFAULT TABLESPACE USERS
  TEMPORARY TABLESPACE TEMP
  PROFILE DEFAULT
  ACCOUNT UNLOCK;
  
-- 2 Grants and Priviledges for User 
WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT RESOURCE TO &1;
  
WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CONNECT  TO &1;
  
WHENEVER SQLERROR EXIT SQL.SQLCODE 
  ALTER USER &1 DEFAULT ROLE ALL;
  
WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT UNLIMITED TABLESPACE TO &1;
  
WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT EXECUTE ON CTXSYS.CTX_DDL TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE DATABASE LINK TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE MATERIALIZED VIEW TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE PROCEDURE TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE PUBLIC SYNONYM TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE ROLE TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE SEQUENCE TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE SYNONYM TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE TABLE TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE TRIGGER TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE TYPE TO &1;

WHENEVER SQLERROR EXIT SQL.SQLCODE 
  GRANT CREATE VIEW TO &1;




select to_char(systimestamp, 'HH24:MI:SS.ff') dcol from dual;
prompt &currTS  Recreated [OK] ;

EXIT SUCCESS
