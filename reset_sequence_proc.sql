CREATE OR REPLACE PROCEDURE Reset_Sequence_proc (
seq_name IN VARCHAR2, table_name IN VARCHAR2, column_name IN VARCHAR2) AS

currentrows INTEGER;
startvalue INTEGER;
cval INTEGER;
inc_by VARCHAR2(25);

BEGIN
  EXECUTE IMMEDIATE 'ALTER SEQUENCE ' ||seq_name||' MINVALUE 0';

  EXECUTE IMMEDIATE 'SELECT ' ||seq_name ||'.NEXTVAL FROM dual'
  INTO cval;

  EXECUTE IMMEDIATE 'SELECT count(' ||column_name || ') FROM ' || table_name
  INTO currentrows;

  IF currentrows > 0 THEN
      EXECUTE IMMEDIATE 'SELECT max(' ||column_name || ') FROM ' || table_name
      INTO startvalue;
  ELSE
      startvalue := 1;
  END IF;

  cval := startvalue - cval;
  IF cval < 0 THEN
    inc_by := ' INCREMENT BY -';
    cval:= ABS(cval);
  ELSE
    inc_by := ' INCREMENT BY ';
  END IF;
  
  IF cval <> 0 THEN
      EXECUTE IMMEDIATE 'ALTER SEQUENCE ' || seq_name || inc_by ||
      cval;

      EXECUTE IMMEDIATE 'SELECT ' ||seq_name ||'.NEXTVAL FROM dual'
      INTO cval;

      EXECUTE IMMEDIATE 'ALTER SEQUENCE ' || seq_name ||
      ' INCREMENT BY 1';
  END IF;

END Reset_Sequence_proc;
/
