create or replace package pkg_log is

  -- Author  : Mykola Solonskyi
  -- Created : 18.03.2008 19:00:13
  -- Purpose : Single log table support

---------------------------------------
procedure sp_log_message(pv_module    in varchar2,
                         pv_text      in varchar2 default null,
                         pv_clob      in clob default null,
                         pv_type      in varchar2 default 'I',
                         pn_qty       in number default null,
                         pb_is_output in boolean default false);

end pkg_log;
/

create or replace package body pkg_log is

---------------------------------------
procedure sp_log_message(pv_module    in varchar2,
                         pv_text      in varchar2 default null,
                         pv_clob      in clob default null,
                         pv_type      in varchar2 default 'I',
                         pn_qty       in number default null,
                         pb_is_output in boolean default false)
as
  pragma autonomous_transaction;
  vv_sid varchar2(20);
  vv_value varchar2(32767) := null;
begin
  select sys_context('USERENV', 'SID') into vv_sid from dual;
  --
  if pb_is_output = true then
    vv_value := 'SID: ' || vv_sid;
    if (pv_text is not null) then
      if (length(vv_value) > 0) then
        vv_value := vv_value || ', ';
      end if;
      vv_value := vv_value || 'text: ' || pv_text;
    end if;
    --
    if (pv_clob is not null) then
      if (length(vv_value) > 0) then
        vv_value := vv_value || ', ';
      end if;
      vv_value := vv_value || 'clob: ' || substr(pv_clob, 1, 4000);
    end if;
    --
    if (pn_qty is not null) then
      if (length(vv_value) > 0) then
        vv_value := vv_value || ', ';
      end if;
      vv_value := vv_value || 'qty: ' || pn_qty;
    end if;
    --
    dbms_output.put_line(substr(vv_value, 1, 4000));
  end if;
  --
  insert into log(module, text, clob_text, type, sid, qty)
  values (pv_module, pv_text, pv_clob, upper(pv_type), nvl(vv_sid, 0), pn_qty);
  commit;
exception
  when others then rollback;
end sp_log_message;

end pkg_log;
/
