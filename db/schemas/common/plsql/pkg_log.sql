create or replace package pkg_log is

  -- Author  : Mykola Solonskyi
  -- Created : 18.03.2008 19:00:13
  -- Purpose : Single log table support

---------------------------------------
gn_batch_id number;
---------------------------------------
procedure sp_log_message(pv_text      in varchar2 default null,
                         pv_clob      in clob default null,
                         pv_type      in varchar2 default 'I',
                         pn_qty       in number default null,
                         pb_is_output in boolean default false);

---------------------------------------
-- returns batches.id into pkg_log.gn_batch_id
procedure sp_start_batch(pv_module     in batches.module%type,
                         pv_parameters in batches.parameters%type default null);

---------------------------------------
procedure sp_finish_batch_successfully;

---------------------------------------
procedure sp_finish_batch_with_errors;

---------------------------------------
procedure sp_finish_batch_with_warnings;

end pkg_log;
/
create or replace package body pkg_log is

gc_in_progress             batches.status%type := 'IN_PROGRESS';
gc_completed_successfully  batches.status%type := 'COMPLETED_SUCCESSFULLY';
gc_completed_with_warnings batches.status%type := 'COMPLETED_WITH_WARNINGS';
gc_completed_with_errors   batches.status%type := 'COMPLETED_WITH_ERRORS';

---------------------------------------
procedure sp_start_batch(pv_module     in batches.module%type,
                         pv_parameters in batches.parameters%type default null)
as
  pragma autonomous_transaction;
  vn_sid       number;
  vn_sessionid number;
begin
  if (gn_batch_id is not null) then
    raise_application_error(-20100, 'BATCH_ID = ' || gn_batch_id || '. Can not start new batch');
  end if;
  --
  select sys_context('USERENV', 'SID'), sys_context('USERENV', 'SESSIONID'), seq_common.nextval
  into vn_sid, vn_sessionid, gn_batch_id
  from dual;
  --
  insert into batches(id, module, parameters, sid, serial#)
  values (gn_batch_id, pv_module, pv_parameters, vn_sid, vn_sessionid);
  --
  commit;
  --
  pkg_log.sp_log_message(pv_text => 'start');
exception
  when others then rollback;
end sp_start_batch;

---------------------------------------
procedure sp_finish_batch_successfully
as
  pragma autonomous_transaction;
begin
  if (gn_batch_id is null) then
    raise_application_error(-20101, 'GN_BATCH_ID is null. Can not finish empty batch');
  end if;
  --
  update batches
    set finish_dtm = systimestamp,
        status     = pkg_log.gc_completed_successfully
  where id = gn_batch_id;
  --
  commit;
  --
  pkg_log.sp_log_message(pv_text => 'completed successfully');
  --
  gn_batch_id := null;
exception
  when others then rollback;
end sp_finish_batch_successfully;

---------------------------------------
procedure sp_finish_batch_with_errors
as
  pragma autonomous_transaction;
begin
  if (gn_batch_id is null) then
    raise_application_error(-20101, 'GN_BATCH_ID is null. Can not finish empty batch');
  end if;
  --
  update batches
    set finish_dtm = systimestamp,
        status     = pkg_log.gc_completed_with_errors
  where id = gn_batch_id;
  --
  commit;
  --
  gn_batch_id := null;
exception
  when others then rollback;
end sp_finish_batch_with_errors;

---------------------------------------
procedure sp_finish_batch_with_warnings
as
  pragma autonomous_transaction;
begin
  if (gn_batch_id is null) then
    raise_application_error(-20101, 'GN_BATCH_ID is null. Can not finish empty batch');
  end if;
  --
  update batches
    set finish_dtm = systimestamp,
        status     = pkg_log.gc_completed_with_warnings
  where id = gn_batch_id;
  --
  commit;
  --
  gn_batch_id := null;
exception
  when others then rollback;
end sp_finish_batch_with_warnings;

---------------------------------------
procedure sp_log_message(pv_text      in varchar2 default null,
                         pv_clob      in clob default null,
                         pv_type      in varchar2 default 'I',
                         pn_qty       in number default null,
                         pb_is_output in boolean default false)
as
  pragma autonomous_transaction;
  vv_value varchar2(32767) := null;
begin
  if pb_is_output = true then
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
  insert into log(batch_id, text, clob_text, type, qty)
  values (gn_batch_id, pv_text, pv_clob, upper(pv_type), pn_qty);
  commit;
exception
  when others then rollback;
end sp_log_message;

end pkg_log;
/
