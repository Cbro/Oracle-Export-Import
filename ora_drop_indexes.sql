SET FEEDBACK OFF
SET VERIFY OFF

-- At this point I have all the list of tables in my global table 
-- I also know the table generation scripts to run!
-- Check to see if the index exists and then drop the user
prompt Verifying index existence and started DROP process ;
WHENEVER SQLERROR EXIT SQL.SQLCODE
declare
  i integer;
begin
  begin -- Exception Block
  for x in (  select ui.index_name
    from   global_table_list t,
           user_indexes ui
    where  ui.table_name = t.table_name
    and    ui.table_owner = '&&1'
    and    ui.index_type != 'LOB'
    and    ui.index_name not like 'PK_%' ) 
  loop
      execute immediate 'DROP INDEX '|| x.index_name ;
  end loop; 
  dbms_output.put_line('');	
  exception
        when others then
	     raise; 
  end;   
end;
/

prompt Drop index process completed [OK]


-- Run the list of tables to create them
-- @tables_list.sql
