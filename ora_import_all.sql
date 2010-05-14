-- Created on 13/10/2008 by MKAUL 

SET LINESIZE 150
SET SERVEROUTPUT on SIZE 1000000 FORMAT WRAPPED
SET FEEDBACK OFF 
SET VERIFY OFF 


WHENEVER SQLERROR EXIT SQL.SQLCODE
declare 
begin
  -- Call database procedure to reload data back in!
  omni_load_proc(pSchemaOwner => '&1', pDBDirectory => 'ORA_LOAD');
end;
/
EXIT SUCCESS