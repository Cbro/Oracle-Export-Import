-- Created on 17/10/2008 by MKAUL 
SET LINESIZE 150
SET SERVEROUTPUT on SIZE 1000000 FORMAT WRAPPED
SET FEEDBACK OFF 
SET VERIFY OFF 


WHENEVER SQLERROR EXIT SQL.SQLCODE
declare 
begin
  omni_unload_pkg.unload_selected_tables(p_owner => '&1', p_tname_list => '&2');
end;
/

EXIT SUCCESS