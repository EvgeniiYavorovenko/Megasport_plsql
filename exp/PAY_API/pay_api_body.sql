create or replace PACKAGE body pay_api AS

    /*
        Перевірка на доступність повторної оплати.
        Запускає перерахунок цін по товарам та записує нові ціни. 
        При успішному перерахунку записуються нові юр. лиця для оплати, при помилці вписується null
    */
    procedure check_repit_pay (
        v_ordh in number
    )AS
        ord ordh%rowtype;
        v_clob clob;
        l_clob clob;
        v_qn number;
        v_domain varchar2(200) := megasport.api_host_pkg.get(v_id=>'msapi_apex_rest');
        v_host varchar2(100) :=v_domain||'/ords/basket/v2/recalculate';
        v_bonus_in varchar2(500);
        v_bonus_out varchar2(500);
        v_err varchar2(3900);  
        v_totalprice number(19,2);  
        v_cnt number;
        v_price number(19,2);
        tv apex_json.t_values;
        ords_r ord_s%rowtype;
        v_json clob;
        v_error number;

        cursor cur2 is
            select art, gds, sizes, sum(cnt) cnt from ord_s
            where ordh = v_ordh
            group by art, gds, sizes;  
    BEGIN
        select * into ord from exp.ordh where id=v_ordh;

        -- формуєм, як строку, а не через apex_json.write(), щоб не зломати формат відповіді на запит
        v_json := '
        {"card":"'||ord.card||'",
        "dat":"'||sysdate||'",
        "trpoint": 213,
        "promo": "'||ord.promo||'",
        "paidtp": '||ord.paidtp||',
        "deliveryType": '||ord.delivery_tp||',
        "location": "web",
        "tp": "create",  
        "orderrows": [';
        for c2 in cur2 loop
            v_json := v_json || '   
            {
                "art": "'||c2.art||'",
                "cnt": '||c2.cnt||',
                "sizes": "'||c2.sizes||'"
            },';
        end loop;

        -- забираємо зайву кому
        v_json := substr(v_json,1,length(v_json)-1);

        v_json := v_json || ']
        }';

        apex_web_service.g_request_headers(1).name := 'Content-Type';
        apex_web_service.g_request_headers(1).value := 'application/json';

        l_clob := apex_web_service.make_rest_request(
            p_url => v_host,
            p_http_method => 'POST',
            p_body=>v_json
        );

        --dbms_output.put_line(v_json);
        --dbms_output.put_line(l_clob);

        --парсим відповідь
        apex_json.parse(tv,l_clob);  
        v_error := apex_json.get_number(p_path=>'err_code' ,p_values=>tv);

        -- якщо відповідь з помилкою зануляєм Юр. лиця, щоб заборонити повторну оплату
        if v_error is not null then
            update ord_s set 
                jur = null,
                lkey = null
            where ordh = v_ordh;
            commit;
        else
            v_qn := apex_json.get_count(p_path=>'orderrows',p_values=>tv);
            for i in 1..v_qn loop
                ords_r.price := apex_json.get_number(p_path=>'orderrows[%d].price',p0=> i ,p_values=>tv);
                ords_r.promo_disc := apex_json.get_number(p_path=>'orderrows[%d].promo_disc',p0=> i ,p_values=>tv);
                ords_r.disc := apex_json.get_number(p_path=>'orderrows[%d].disc',p0=> i ,p_values=>tv);
                ords_r.bonus_disc := apex_json.get_number(p_path=>'orderrows[%d].bonus_disc',p0=> i ,p_values=>tv);
                ords_r.act_disc := apex_json.get_number(p_path=>'orderrows[%d].act_disc',p0=> i ,p_values=>tv);
                ords_r.card_disc := apex_json.get_number(p_path=>'orderrows[%d].card_disc',p0=> i ,p_values=>tv);
                ords_r.jur := apex_json.get_varchar2(p_path=>'orderrows[%d].jur',p0=> i ,p_values=>tv);
                ords_r.lkey := apex_json.get_varchar2(p_path=>'orderrows[%d].lkey',p0=> i ,p_values=>tv);
                ords_r.art:=apex_json.get_varchar2(p_path=>'orderrows[%d].art',p0=> i,p_values=>tv);  
                ords_r.sizes :=apex_json.get_varchar2(p_path=>'orderrows[%d].sizes',p0=> i,p_values=>tv);
                v_cnt :=apex_json.get_varchar2(p_path=>'orderrows[%d].cnt',p0=> i,p_values=>tv);

                update ord_s set 
                    price = round(ords_r.price/v_cnt,1), 
                    promo_disc = round(ords_r.promo_disc/v_cnt,1), 
                    disc = round(ords_r.disc/v_cnt,1),
                    bonus_disc = round(ords_r.bonus_disc/v_cnt,1), 
                    act_disc = round(ords_r.act_disc/v_cnt,1),
                    card_disc = round(ords_r.card_disc/v_cnt,1),
                    jur = ords_r.jur,
                    lkey = ords_r.lkey
                where ordh = v_ordh and art = ords_r.art and sizes=ords_r.sizes;    
            end loop;   

            v_totalprice:=apex_json.get_number(p_path=>'totalprice',p_values=>tv);
            v_bonus_in :=apex_json.get_varchar2(p_path=>'bonus_in',p_values=>tv);
            v_bonus_out :=apex_json.get_varchar2(p_path=>'bonus_out',p_values=>tv);

            update ordh set 
                totalprice=v_totalprice,
                bonus_in=v_bonus_in,
                bonus_out=v_bonus_out
            where id = v_ordh;
            commit;
        end if; 
    END check_repit_pay;

    /*
        Формування відповіді на перевірку.
        Формат, як відповідь при створенні замовлення.

    */
    PROCEDURE get_response_calc(
        v_ordh in number, 
        v_err out clob
    ) as

        CURSOR cur2 IS 
            SELECT 
                ordh,
                art,
                sum(cnt) cnt,
                sizes,
                sum(price) price,
                model,
                section,
                brand,
                url,
                gds,
                sum(promo_disc) promo_disc,
                pricetp,
                sum(disc) disc,
                sum(bonus_disc) bonus_disc,
                sum(act_disc) act_disc,
                sum(card_disc) card_disc,
                jur,
                lkey
            FROM ord_s o 
            WHERE ordh = v_ordh
            group by 
                ordh,
                art,
                sizes,
                model,
                section,
                brand,
                url,
                gds,
                pricetp,
                jur,
                lkey
            order by jur;

        ord ordh%rowtype;
        v_price1 number;
        v_rrp number;
        v_div number;
        v_sum_recalc_disc number := 0;
        v_sum_disc number := 0;
        v_sum_card_disc number := 0;
        v_sum_act_disc number := 0;
        v_sum_bonus_disc number := 0;
        v_sum_promo_disc number := 0;

        v_mono_resp clob;
        tv             apex_json.t_values;
        v_monobank varchar2(100);
        v_clob clob;
        v_isvalid number;
    BEGIN 
        SELECT * INTO ord 
        FROM ordh 
        WHERE id = v_ordh; 

        select count(*) into v_isvalid
        from ord_s
        where ordh=v_ordh and lkey is null;

        apex_json.open_object;

        -- Якщо один з товарів без вказаного юр. лиця - забороняємо повторну оплату
        if nvl(v_isvalid,0) = 0 then
            apex_json.write('valid_to_pay',true); 
        else
            apex_json.write('valid_to_pay',false); 
        end if;

        apex_json.write('id',ord.id); 
        apex_json.write('name',ord.name); 
        apex_json.write('surrname',ord.surname); 
        apex_json.write('complete',ord.complete); 
        apex_json.write('phone',ord.phone); 
        apex_json.write('bonus_in',ord.bonus_in); 
        apex_json.write('bonus_out',ord.bonus_out); 
        apex_json.write('email',ord.email); 
        apex_json.write('promo',ord.promo);
        apex_json.write('stat_id', 2);
        apex_json.write('stat_ru', 'Ваш заказ принят, ожидает обработку');
        apex_json.write('stat_ua', 'Ваше замовлення прийняте, очікує опрацювання');
        apex_json.write('stat_grp_id', 1);
        apex_json.write('stat_grp_ru', 'новый, ожидает оплаты');
        apex_json.write('stat_grp_ua', 'нове, очікує оплати');
        apex_json.open_array('orderrows'); 
        FOR c2 IN cur2 LOOP 
            apex_json.open_object; 
            apex_json.write('id',c2.gds);
            apex_json.write('art',c2.art); 
            apex_json.write('cnt',c2.cnt); 
            apex_json.write('sizes',c2.sizes);

            v_rrp := round(megasport.fprice(c2.gds,6),1); --
            apex_json.write('cost',v_rrp); --

            v_price1 := round(megasport.fprice(c2.gds,2),1); --
            apex_json.write('recalc_cost',v_price1);  --

            apex_json.write('price',c2.price); 
            apex_json.write('price_for_one',round(c2.price/c2.cnt,1)); 
            v_div := v_rrp - v_price1; --
            apex_json.write('recalc_disc',v_div*c2.cnt); --

            apex_json.write('model',c2.model); 
            apex_json.write('section',c2.section); 
            apex_json.write('brand',c2.brand); 
            apex_json.write('disc',nvl(c2.disc,0)); 
            apex_json.write('bonus_disc',nvl(c2.bonus_disc,0));
            apex_json.write('act_disc',nvl(c2.act_disc,0)); 
            apex_json.write('card_disc',nvl(c2.card_disc,0));
            apex_json.write('promo_disc',nvl(c2.promo_disc,0));
            apex_json.write('total_gds_disc',c2.disc + v_div*c2.cnt);
            apex_json.write('jur',c2.jur);
            apex_json.write('lkey',c2.lkey);
            if ord.paidtp = 5 then
                v_mono_resp := apex_web_service.make_rest_request(
                    p_url => 'http://msboard.apex.rest/ords/exp/basket/mono_lkey/'||c2.jur, 
                    p_http_method => 'POST', 
                    p_body => null
                );

                apex_json.parse(tv,v_mono_resp);
                v_monobank := apex_json.get_varchar2(p_path => 'lkey',p_values => tv);
                apex_json.write('monobank_lkey',v_monobank);
            end if;

            v_sum_recalc_disc := v_sum_recalc_disc + (v_div*c2.cnt);

            v_sum_disc          := v_sum_disc + nvl(c2.disc,0);
            v_sum_card_disc     := v_sum_card_disc + nvl(c2.card_disc,0);
            v_sum_act_disc      := v_sum_act_disc + nvl(c2.act_disc,0);
            v_sum_bonus_disc    := v_sum_bonus_disc + nvl(c2.bonus_disc,0);
            v_sum_promo_disc    := v_sum_promo_disc + nvl(c2.promo_disc,0);

            apex_json.close_object;
        END LOOP;
        apex_json.close_array; 

        apex_json.write('totalprice',ord.totalprice); 
        apex_json.write('total_disc',v_sum_disc);
        apex_json.write('total_recalc',v_sum_recalc_disc + v_sum_disc);
        apex_json.write('card_disc',v_sum_card_disc);
        apex_json.write('act_disc',v_sum_act_disc);
        apex_json.write('bonus_disc',v_sum_bonus_disc);
        apex_json.write('totalpromo',v_sum_promo_disc);

        apex_json.close_object;
    END get_response_calc;

end pay_api;