
prompt Verifying sequence existence and started DROP process ;
WHENEVER SQLERROR EXIT SQL.SQLCODE
declare
  i integer;
begin
  begin -- Exception Block
  
  for x in (	select t.seq_name
		from   global_seq_list t,
		       user_sequences us
		where  us.sequence_name = t.seq_name ) loop
      execute immediate 'DROP SEQUENCE '|| x.seq_name ;
      -- dbms_output.put_line('Dropped Sequence --> '|| x.seq_name );
  end loop; 
  dbms_output.put_line('');	
  exception
        when others then
	     raise; 
  end;   
end;
/

prompt Drop sequence process completed [OK]