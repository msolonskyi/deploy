create or replace package pkg_utils is

  -- Author  : Mykola Solonskyi
  -- Created : 08/10/2014 12:21:04
  -- Purpose : Utils for autopatic deploy table from XML structure

---------------------------------------
CRLF                        constant char(2)     := chr(13) || chr(10);
c_null_varchar_substitution constant varchar2(4) := chr(7) || chr(7) || chr(8) || chr(7);
c_null_number_substitution  constant number      := 1e12;
c_null_date_substitution    constant date        := to_date('01.01.1800', 'dd.mm.yyyy');

---------------------------------------
function sf_get_data_default(
  pv_table_owner in varchar2,
  pv_table_name  in varchar2,
  pv_column_name in varchar2)
return varchar2;

---------------------------------------
function nvl2(
  pv_value                 in varchar2,
  pv_new_value_if_not_null in varchar2,
  pv_new_value_if_null     in varchar2)
return varchar2 deterministic;

---------------------------------------
function sf_normalize_char_used(pv_char_used in varchar2) return varchar2 deterministic;

---------------------------------------
function sf_get_search_condition(
  pv_owner           in varchar2,
  pv_constraint_name in varchar2)
return varchar2;

end pkg_utils;
/

create or replace package body pkg_utils is

---------------------------------------
function nvl2(
  pv_value                 in varchar2,
  pv_new_value_if_not_null in varchar2,
  pv_new_value_if_null     in varchar2)
return varchar2 deterministic
is
begin
  if pv_value is not null then
    return(pv_new_value_if_not_null);
  else
    return(pv_new_value_if_null);
  end if;
end nvl2;

---------------------------------------
function sf_get_data_default(
  pv_table_owner in varchar2,
  pv_table_name  in varchar2,
  pv_column_name in varchar2)
return varchar2
as
  l_query     varchar2(1000) := 'select data_default from all_tab_cols where owner = :towner and table_name = :tname and column_name = :cname';
  l_from_byte number := 0;
  l_for_bytes number := 1000;
  l_cursor    integer default dbms_sql.open_cursor;
  l_long_val  long;
  l_buflen    integer;
  l_ignore    number;
begin
  dbms_sql.parse(l_cursor, l_query, dbms_sql.native);
  dbms_sql.bind_variable(l_cursor, ':towner', pv_table_owner);
  dbms_sql.bind_variable(l_cursor, ':tname', pv_table_name);
  dbms_sql.bind_variable(l_cursor, ':cname', pv_column_name);
  dbms_sql.define_column_long(l_cursor, 1);
  l_ignore := dbms_sql.execute(l_cursor);
  if (dbms_sql.fetch_rows(l_cursor) > 0) then
    dbms_sql.column_value_long(
      c            => l_cursor,
      position     => 1,
      length       => l_for_bytes,
      offset       => l_from_byte,
      value        => l_long_val,
      value_length => l_buflen);
  end if;
  dbms_sql.close_cursor(l_cursor);
  return trim(l_long_val);
exception
  when others then
    if dbms_sql.is_open(l_cursor) then
      dbms_sql.close_cursor(l_cursor);
    end if;
    raise;
end sf_get_data_default;

---------------------------------------
function sf_normalize_char_used(pv_char_used in varchar2) return varchar2 deterministic
is
  vv_char_used varchar2(4);
begin
  vv_char_used := upper(trim(pv_char_used));
  case
    when vv_char_used = 'B'    then vv_char_used := 'BYTE';
    when vv_char_used = 'BYTE' then vv_char_used := 'BYTE';
    when vv_char_used = 'C'    then vv_char_used := 'CHAR';
    when vv_char_used = 'CHAR' then vv_char_used := 'CHAR';
    else                            vv_char_used := null;
  end case;
  return vv_char_used;
end sf_normalize_char_used;

---------------------------------------
function sf_get_search_condition(
  pv_owner           in varchar2,
  pv_constraint_name in varchar2)
return varchar2
as
  l_query     varchar2(1000) := 'select search_condition from all_constraints where owner = :owner and constraint_name = :constraint_name';
  l_from_byte number := 0;
  l_for_bytes number := 1000;
  l_cursor    integer default dbms_sql.open_cursor;
  l_long_val  long;
  l_buflen    integer;
  l_ignore    number;
begin
  dbms_sql.parse(l_cursor, l_query, dbms_sql.native);
  dbms_sql.bind_variable(l_cursor, ':owner', pv_owner);
  dbms_sql.bind_variable(l_cursor, ':constraint_name', pv_constraint_name);
  dbms_sql.define_column_long(l_cursor, 1);
  l_ignore := dbms_sql.execute(l_cursor);
  if (dbms_sql.fetch_rows(l_cursor) > 0) then
    dbms_sql.column_value_long(
      c            => l_cursor,
      position     => 1,
      length       => l_for_bytes,
      offset       => l_from_byte,
      value        => l_long_val,
      value_length => l_buflen);
  end if;
  dbms_sql.close_cursor(l_cursor);
  return trim(l_long_val);
exception
  when others then
    if dbms_sql.is_open(l_cursor) then
      dbms_sql.close_cursor(l_cursor);
    end if;
    raise;
end sf_get_search_condition;

end pkg_utils;
/
