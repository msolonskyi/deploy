create or replace view vw_log as
select dtm, batch_id, text, clob_text, type, qty, id, start_stm, finish_dtm, module, parameters, status, sid, serial#
from batches b, log l
where b.id = l.batch_id
/
