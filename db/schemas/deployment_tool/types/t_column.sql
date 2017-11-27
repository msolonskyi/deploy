create or replace type t_column is object(
  name               varchar2(30),
  type               varchar2(30),
  char_length        number,
  data_precision     number,
  data_scale         number,
  char_used          varchar(4),
  nullable           varchar(1),
  default_value      varchar2(30),
  virtual_expression varchar2(4000),
  comments           varchar2(4000),
map member function equals return varchar2,
member function mf_type_to_string return varchar2,
member function mf_get_add_column_string return varchar2,
member function mf_get_modify_column_string return varchar2,
member function mf_get_drop_column_string return varchar2);
/

create or replace type body t_column is

---------------------------------------
map member function equals return varchar2 as
begin
  return upper(name || '_' || type || '_' || char_length || '_' || data_precision || '_' || data_scale || '_' || char_used || '_' || nullable || '_' || default_value || '_' || virtual_expression);
end equals;

---------------------------------------
member function mf_type_to_string return varchar2 as
  vv_value varchar2(4000);
begin
  if self.virtual_expression is not null then
    -- virtual column
    vv_value := 'as (' || self.virtual_expression || ')';
  elsif self.type is not null then
    -- normal column
    case
      when self.data_precision is not null and nvl(self.data_scale, 0) > 0 then vv_value := self.type || '(' || self.data_precision || ',' || self.data_scale || ')';
      when self.data_precision is not null and nvl(self.data_scale, 0) = 0 then vv_value := self.type || '(' || self.data_precision || ')';
      when self.data_precision is null and self.data_scale is not null and self.type not like 'TIMESTAMP%' then vv_value := self.type || '(*,' || self.data_scale || ')';
      when self.char_length > 0 then vv_value := self.type || '(' || self.char_length || ' ' || self.char_used || ')';
      else vv_value := self.type;
    end case;
  else
    -- issue
    raise_application_error(-20001, 't_columns.sf_type_to_string unexpected parameters. name => ' || self.name || ', type => ' || self.type || ', char_length => ' || self.char_length || ', data_precision => ' || self.data_precision || ', data_scale => ' || self.data_scale || ', char_used => ' || self.char_used || ', nullable => ' || self.nullable || ', default_value => ' || self.default_value || ', virtual_expression => ' || self.virtual_expression);
  end if;
  return vv_value;
end mf_type_to_string;

---------------------------------------
member function mf_get_add_column_string return varchar2 as
  vv_value varchar2(4000);
begin
  vv_value := pkg_utils.CRLF || '  ' || self.name || ' ' || self.mf_type_to_string;
  if self.default_value is not null then
    vv_value := vv_value || ' default ' || self.default_value;
  end if;
  if self.nullable = 'N' then
    vv_value := vv_value || ' not null';
  end if;
  return vv_value;
end mf_get_add_column_string;

---------------------------------------
member function mf_get_modify_column_string return varchar2 as
  vv_value varchar2(4000);
begin
  vv_value := pkg_utils.CRLF || '  ' || self.name;
  -- type
  if ((self.char_length is null) and (self.data_precision is null) and (self.data_scale is null) and (self.char_used is null)) then
    null;
  else
    vv_value := vv_value || ' ' || self.mf_type_to_string;
  end if;
  -- dafault
  if self.default_value is not null then
    vv_value := vv_value || ' default ' || self.default_value;
  end if;
  -- nullable
  if self.nullable is not null then
    if self.nullable = 'N' then
      vv_value := vv_value || ' not null';
    else
      vv_value := vv_value || ' null';
    end if;
  end if;
  return vv_value;
end mf_get_modify_column_string;

---------------------------------------
member function mf_get_drop_column_string return varchar2 as
begin
  return pkg_utils.CRLF || '  ' || self.name;
end mf_get_drop_column_string;

end;
/
