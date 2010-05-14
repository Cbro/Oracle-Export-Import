-- Created on 17/10/2008 by MKAUL

SET LINESIZE 150
SET SERVEROUTPUT on SIZE 1000000 FORMAT WRAPPED
SET FEEDBACK OFF 
SET VERIFY OFF 

WHENEVER SQLERROR EXIT SQL.SQLCODE 
declare 
  -- Local variables here
  i integer;
begin
  -- Test statements here
  omni_unload_pkg.main(p_owner => '&1');
end;
/

EXIT SUCCESS