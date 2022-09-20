create or replace package body paid_pkg as
    -- запис інформації про оплату розділеного платежу
    procedure set_paid_split(v_clob in clob, v_err out varchar2) 
    as 
        tv apex_json.t_values;
        r_h split_paidh%rowtype;
        r_s split_paids%rowtype;
        v_cnt number;
    begin
        apex_json.parse(tv,v_clob);

        r_h.TRANSACTION_ID := apex_json.get_varchar2(p_path => 'transactionId', p_values => tv);
        r_h.SPLIT_TRANSACTION_ID := apex_json.get_varchar2(p_path => 'splitTransactionId', p_values => tv);
        r_h.PAYMENT_ID := apex_json.get_number(p_path => 'payment_id', p_values => tv);
        r_h.ORDER_STATUS := apex_json.get_varchar2(p_path => 'order_status', p_values => tv);
        r_h.ORDH := to_number(apex_json.get_varchar2(p_path => 'orderId', p_values => tv));

        insert into split_paidh
        values r_h
        return id into r_s.doc;

        v_cnt := apex_json.get_count(p_path => 'transactions',p_values => tv);
        for i in 1..nvl(v_cnt,0) LOOP
            r_s.STATUS := apex_json.get_varchar2(p_path => 'transactions[%d].status', p0 => i, p_values => tv);
            r_s.AMOUNT := apex_json.get_varchar2(p_path => 'transactions[%d].amount', p0 => i, p_values => tv);
            r_s.MERCHANT_ID := apex_json.get_varchar2(p_path => 'transactions[%d].merchant_id', p0 => i, p_values => tv);

            insert into split_paids
            values r_s;

            r_s.STATUS := null;
            r_s.AMOUNT := null;
            r_s.MERCHANT_ID := null;
        end loop;

        commit;
    exception when others then
        v_err := sqlerrm;
        rollback;
    end set_paid_split;

end paid_pkg;