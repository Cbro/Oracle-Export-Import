CREATE OR REPLACE PACKAGE
--
--  Module    : OMNI_UNLOAD_PKG (Spec)
--  Author    : CBro
--  Created   : 13/10/2008 10:34:05
--  Purpose   : To unload local XE database into CSV files for developers sandboxes
--
--  Revision History
--
--
 OMNI_UNLOAD_PKG AS

  --
  procedure main(p_owner in varchar2);

  function find_pk(p_tname in varchar2) return varchar2;

  procedure unload_selected_tables(p_owner      in varchar2,
                                   p_tname_list in varchar2);

  function unload(p_query      in varchar2 default NULL,
                  p_cols       in varchar2 default '*',
                  p_town       in varchar2 default USER,
                  p_tname      in varchar2,
                  p_dbdir      in varchar2,
                  p_filename   in varchar2,
                  p_separator  in varchar2 default ',',
                  p_enclosure  in varchar2 default '||',
                  p_terminator in varchar2 default '^^^^',
                  p_header     in varchar2 default 'YES') return number;
  --

end omni_unload_pkg;
/


CREATE OR REPLACE PACKAGE BODY
--
--  Module    : OMNI_UNLOAD_PKG (Spec)
--  Author    : Manu Kaul
--  Created   : 13/10/2008 10:34:05
--  Purpose   : To unload local XE database into CSV files for developers sandboxes
--
--  Revision History
--
--  Date        Who             Version         Details
--  =========== =============== =============== =======================================
--  13/10/2008  Manu Kaul       V1.0.MKA.0.0    Initial Version
--
 OMNI_UNLOAD_PKG AS
  --
  g_theCursor integer default dbms_sql.open_cursor;
  g_descTbl   dbms_sql.desc_tab;
  g_Tblname   varchar2(30) default NULL;
  vOrderBy    varchar2(1000) default NULL;
  vOrderCount number default 0;

  -- Create a Table Type to store table names
  TYPE myTableType IS TABLE OF VARCHAR2(30);
  --
  -- Function to convert characters to HEX characters! 
  --
  function to_hex(p_str in varchar2) return varchar2 is
  begin
    return to_char(ascii(p_str), 'fm0x');
  end;

  --
  -- Function to qoute the values
  -- 
  function quote(p_str in varchar2, p_enclosure in varchar2) return varchar2 is
  begin
    return p_enclosure || replace(p_str,
                                  p_enclosure,
                                  p_enclosure || p_enclosure) || p_enclosure;
  end;
  --
  --

  -- Function to split the comma separated values into strings and 
  -- place into a PL/SQL table to loop through
  function str2tbl(p_str in varchar2, p_delim in varchar2 default ',')
    return myTableType as
    l_str  long default p_str || p_delim;
    l_n    number;
    l_data myTableType := myTableType();
  begin
    loop
      l_n := instr(l_str, p_delim);
      exit when(nvl(l_n, 0) = 0);
      l_data.extend;
      l_data(l_data.count) := upper(ltrim(rtrim(substr(l_str, 1, l_n - 1))));
      l_str := substr(l_str, l_n + length(p_delim));
    end loop;
    return l_data;
  end;

  procedure unload_selected_tables(p_owner      in varchar2,
                                   p_tname_list in varchar2) as
    vTableList  varchar2(32760) := null;
    vTableArray myTableType;
    vDummy      number := 0;
    l_rows      number;
  begin
    -- Get rid of the space characters and compress the incoming pattern 
    vTableList := regexp_replace(p_tname_list, ' ', '');
  
    -- Loop through the list of tables and split to get the list of tables 
    vTableArray := str2tbl(vTableList);
    <<Table_List>>
    FOR i IN 1 .. vTableArray.COUNT LOOP
      if (vTableArray(i) is not null and length(vTableArray(i)) > 0) then
        -- Check to see if this table even exists in the system 
        begin
          select 1
            into vDummy
            from all_tables a
           where a.owner = p_owner
             and a.table_name = vTableArray(i);
        
          l_rows := unload(p_cols       => '*',
                           p_town       => p_owner,
                           p_tname      => vTableArray(i),
                           p_dbdir      => 'ORA_UNLOAD',
                           p_filename   => vTableArray(i),
                           p_separator  => ',',
                           p_enclosure  => '||',
                           p_terminator => '^^^^',
                           p_header     => 'YES');
        
          dbms_output.put_line('Extracted to CSV file from Table Name = ' ||
                               vTableArray(i));
        exception
          when NO_DATA_FOUND then
            dbms_output.put_line('******* (ERROR) : No matching table was found for --> <' ||
                                 vTableArray(i) ||
                                 '> ... hence skipped!!!');
        end;
      end if;
    END LOOP Table_List;
  
  end unload_selected_tables;

  -- Main Procedure that gets called
  procedure main(p_owner in varchar2) as
    l_rows number;
  begin
  
    begin
      -- Loop through the tables to unload
      for tbl in (select a.table_name
                    from all_tables a
                   where a.owner = p_owner
                     and a.table_name not like 'DR$%'
                     and a.table_name not like 'MLOG$%'
                     and a.table_name not in
                         ('EVENT_OBJ', 'GLOBAL_SEQ_LIST', 'GLOBAL_TABLE_LIST')
                   order by a.table_name) loop
      
        l_rows := unload(p_cols       => '*',
                         p_town       => p_owner,
                         p_tname      => tbl.table_name,
                         p_dbdir      => 'ORA_UNLOAD',
                         p_filename   => tbl.table_name,
                         p_separator  => ',',
                         p_enclosure  => '||',
                         p_terminator => '^^^^',
                         p_header     => 'YES');
        --
        dbms_output.put_line('Extracted to CSV file from Table Name = ' ||
                             tbl.table_name);
      end loop;
    exception
      when others then
        dbms_output.put_line('******* (ERROR) : Error Encountered : ' ||
                             SQLCODE || ': ' || substr(SQLERRM, 1, 200));
        --
        RAISE;
    end; -- End of exception   
  
  end main;

  -- To write CLOBs to File
  procedure clob_to_file(p_dir  in varchar2,
                         p_file in varchar2,
                         p_clob in clob) as
    l_output utl_file.file_type;
    l_amt    number default 32000;
    l_offset number default 1;
    l_length number default nvl(dbms_lob.getlength(p_clob), 0);
  BEGIN
    l_output := utl_file.fopen(p_dir, p_file, 'w', 32760);
    while (l_offset < l_length) loop
      utl_file.put(l_output, dbms_lob.substr(p_clob, l_amt, l_offset));
      utl_file.fflush(l_output);
      l_offset := l_offset + l_amt;
    end loop;
    utl_file.new_line(l_output);
    utl_file.fclose(l_output);
  end;

  -- Function to find the Primary Key column(s) in the tables
  function find_pk(p_tname in varchar2) return varchar2 as
    vPKString varchar2(100) := '';
  begin
    vOrderCount := 0;
    -- Begin by finding out what the Primary Keys Column(s) is/are:1
    for pk in (select ac.table_name, acc.column_name
                 from all_constraints ac, all_cons_columns acc
                where ac.constraint_name = acc.constraint_name
                  and ac.constraint_type = 'P'
                  and ac.table_name = p_tname
                order by ac.table_name, acc.position) loop
      -- Increment Counter
      vOrderCount := vOrderCount + 1;
    
      if (vOrderCount = 1) then
        vPKString := vPKString || pk.column_name;
      else
        vPKString := vPKString || ',' || pk.column_name;
      end if;
    
    end loop;
  
    return vPKString;
  end find_pk;

  --
  -- Uses database directory to unload the values into a CSV formatted file
  --
  function unload(p_query      in varchar2 default NULL,
                  p_cols       in varchar2 default '*',
                  p_town       in varchar2 default USER,
                  p_tname      in varchar2,
                  p_dbdir      in varchar2,
                  p_filename   in varchar2,
                  p_separator  in varchar2 default ',',
                  p_enclosure  in varchar2 default '||',
                  p_terminator in varchar2 default '^^^^',
                  p_header     in varchar2 default 'YES') return number is
    l_query       varchar2(4000);
    l_output      utl_file.file_type;
    l_columnValue varchar2(4000);
  
    l_colCnt       number default 0;
    l_separator    varchar2(10) default '';
    l_cnt          number default 0;
    l_line         long;
    l_datefmt      varchar2(255);
    l_timestampfmt varchar2(255);
  
    l_descTbl dbms_sql.desc_tab;
  
    vTmpClob CLOB;
    -- Filenames for clobs
    vClobFilename varchar2(1000) default '';
    vFileID       number default 0;
  begin
    -- Assign incoming table to global table name
    g_Tblname := p_tname;
    select value
      into l_datefmt
      from nls_session_parameters
     where parameter = 'NLS_DATE_FORMAT';
  
    select value
      into l_timestampfmt
      from nls_session_parameters
     where parameter = 'NLS_TIMESTAMP_FORMAT';
  
    --
    -- Set the date format to a big numeric string. Avoids
    -- all NLS issues 
    --
    execute immediate 'alter session set nls_date_format=''DD-MON-YYYY HH24.MI.SS'' ';
    --
    -- Set the timestamp format to a big numeric string. Avoids
    -- all NLS issues and saves both the time and date.
    --
    execute immediate 'alter session set nls_timestamp_format =''DD-MON-YYYY HH24.MI.SSXFF'' ';
  
    --
    -- Set up an exception block so that in the event of any
    -- error, we can at least reset the date format back.
    --
    declare
      invalid_type EXCEPTION;
    begin
      vOrderBy := find_pk(p_tname);
      --
      -- Parse and describe the query. We reset the
      -- descTbl to an empty table so .count on it
      -- will be reliable.
      --
      if p_query is NULL then
        if (vOrderBy is NULL) then
          l_query := 'select ' || p_cols || ' from ' || p_town || '.' ||
                     p_tname;
        else
          l_query := 'select ' || p_cols || ' from ' || p_town || '.' ||
                     p_tname || ' order by ' || vOrderBy;
        end if;
      else
        l_query := p_query;
      end if;
    
      --
      dbms_sql.parse(g_theCursor, l_query, dbms_sql.native);
      g_descTbl := l_descTbl;
      dbms_sql.describe_columns(g_theCursor, l_colCnt, g_descTbl);
    
      -- Output the column names and types to trap CLOB/BLOB datatypes
      /*      for i in 1 .. g_descTbl.count loop
              dbms_output.put_line('Column name = ' || g_descTbl(i)
                                   .col_name || ' type = ' || g_descTbl(i)
                                   .col_type);
            end loop;
      */
      --
      -- Bind every single column to a varchar2(4000). We don't care
      -- if we are fetching a number or a date or whatever.
      -- Everything can be a string except for a CLOB!
      --
      --vFoundCLOBFlg := 0;
      for i in 1 .. l_colCnt loop
        -- Needs to check for CLOBs
        if (g_descTbl(i).col_type = 112) then
          dbms_sql.define_column(g_theCursor, i, vTmpClob);
        else
          dbms_sql.define_column(g_theCursor, i, l_columnValue, 4000);
        end if;
      end loop;
      --
      -- Run the query - ignore the output of execute. It is only
      -- valid when the DML is an insert/update or delete.
      --
      l_cnt := dbms_sql.execute(g_theCursor);
      --
      -- Open the file to write output to and then write the
      -- delimited data to it.
      --
      l_output := utl_file.fopen(p_dbdir, p_filename || '.csv', 'w', 32760);
      --
      -- Output a column header. This version uses table column comments if they
      -- exist, otherwise it defaults to the actual table column name.
      --
      IF p_header = 'YES' THEN
        l_separator := '';
        l_line      := '';
        for i in 1 .. g_descTbl.count loop
          l_line      := l_line || l_separator ||
                         quote(g_descTbl(i).col_name, p_enclosure);
          l_separator := p_separator;
        end loop;
        l_line := l_line || p_terminator;
        utl_file.put_line(l_output, l_line);
      END IF;
      --
      -- Output data
      --
      vFileID := 0;
      loop
        exit when(dbms_sql.fetch_rows(g_theCursor) <= 0);
        l_separator := '';
        l_line      := null;
      
        -- Each row's columns 
        --vIndex := 1;
      
        <<Columns_Loop>>
        for i in 1 .. l_colCnt loop
          -- Check for CLOB existence
          if (g_descTbl(i).col_type = 112) then
            vFileID       := vFileID + 1;
            vClobFilename := upper(p_tname) || '_' || vFileID || '.clob';
          
            -- Put in the filename into the file 
            l_columnValue := vClobFilename;
          
            -- This is the CLOB value read from the column
            dbms_sql.column_value(g_theCursor, i, vTmpClob);
          
            -- Call the routine to write out the CLOB
            clob_to_file(p_dbdir, vClobFilename, vTmpClob);
            -- Null out the Clob for next one
            vTmpClob := null;
          
          else
            -- All the normal operations
            dbms_sql.column_value(g_theCursor, i, l_columnValue);
          end if;
        
          l_line      := l_line || l_separator ||
                         quote(l_columnValue, p_enclosure);
          l_separator := p_separator;
        end loop Columns_Loop;
        l_line := l_line || p_terminator;
        utl_file.put_line(l_output, l_line);
        l_cnt := l_cnt + 1;
      end loop;
    
      utl_file.fclose(l_output);
      --
      -- Now reset the date format and return the number of rows
      -- written to the output file.
      --
      execute immediate 'alter session set nls_date_format=''' || l_datefmt || '''';
      execute immediate 'alter session set nls_timestamp_format =''' ||
                        l_timestampfmt || '''';
    
      --
      return l_cnt;
    exception
      --
      -- In the event of ANY error, reset the data format and
      -- re-raise the error.
      --
      when invalid_type then
        execute immediate 'alter session set nls_date_format=''' ||
                          l_datefmt || '''';
        execute immediate 'alter session set nls_timestamp_format =''' ||
                          l_timestampfmt || '''';
      
        --
        dbms_output.put_line('******* (ERROR) : Invalid File Type : ' ||
                             SQLCODE || ': ' || substr(SQLERRM, 1, 200));
        --
        RAISE;
      when utl_file.invalid_path then
        execute immediate 'alter session set nls_date_format=''' ||
                          l_datefmt || '''';
        execute immediate 'alter session set nls_timestamp_format =''' ||
                          l_timestampfmt || '''';
      
        --
        dbms_output.put_line('******* (ERROR) : Invalid File Path : ' ||
                             SQLCODE || ': ' || substr(SQLERRM, 1, 200));
        --
        RAISE;
      when others then
        execute immediate 'alter session set nls_date_format=''' ||
                          l_datefmt || '''';
        execute immediate 'alter session set nls_timestamp_format =''' ||
                          l_timestampfmt || '''';
      
        dbms_output.put_line('******* (ERROR) : Unexpected Error while processing table ' ||
                             g_Tblname || ': ' || SQLCODE || ': ' ||
                             substr(SQLERRM, 1, 200));
        --
        RAISE;
      
    end;
  end unload;

--
--
end omni_unload_pkg;
/
