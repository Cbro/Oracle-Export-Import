SET FEEDBACK OFF
SET VERIFY OFF

-- At this point I have all the list of tables in my global table 
-- I also know the table generation scripts to run!
-- Check to see if the user exists and then drop the user


WHENEVER SQLERROR EXIT SQL.SQLCODE
declare
  drop_file UTL_FILE.FILE_TYPE;
begin
  begin -- Exception Block
  -- Using UTL_FILE open up the file to write the drop scripts into
  drop_file := 	utl_file.fopen( 'DROP_SQL_DIR','drop-tables-that-exist.sql','W');
  utl_file.put_line(drop_file, 'SET FEEDBACK OFF');
  utl_file.put_line(drop_file, 'SET VERIFY OFF');

  -- Loop through and generate drops for tables that already exist! 
  for x in (	select ut.table_name
		from   global_table_list t,
		       user_tables ut
		where  ut.table_name = t.table_name ) loop
      -- execute immediate 'DROP TABLE '|| x.table_name ||' CASCADE CONSTRAINTS';
      -- dbms_output.put_line('Dropped Table --> '|| x.table_name );
      utl_file.put_line(drop_file, 'DROP TABLE '|| x.table_name ||' CASCADE CONSTRAINTS;');	
  end loop; 
  utl_file.fclose(drop_file);
  exception
        when others then
	     raise; 
  end;   
end;
/


