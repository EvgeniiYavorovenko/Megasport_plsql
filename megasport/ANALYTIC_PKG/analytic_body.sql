create or replace package body analytic_pkg as

-- продажі -> по магазинам
function sales_by_shops(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined
as  
    v_d1_1 date;
    v_d2_1 date;
    v_d1_2 date;
    v_d2_2 date;
    
    return_row_p1 sales_by_shops_r_char;
    return_row_p2 sales_by_shops_r_char;
    return_row_diff sales_by_shops_r_char;
    return_row_jur_p1 sales_by_shops_r_char;
    return_row_jur_p2 sales_by_shops_r_char;
    return_row_total_p1 sales_by_shops_r_char;
    return_row_total_p2 sales_by_shops_r_char;
    
    is_p1 number := 0;
    is_p2 number := 0;
    l_url varchar2(4000);
begin

    if d1_1 > d1_2 then
        v_d1_1 := d1_2;
        v_d2_1 := d2_2;
        
        v_d1_2 := d1_1;
        v_d2_2 := d2_1;
    else
        v_d1_2 := d1_2;
        v_d2_2 := d2_2;
        
        v_d1_1 := d1_1;
        v_d2_1 := d2_1;
    end if;
    
    return_row_total_p1.name := '<div width="100%" ><b>Всього'||' '|| v_d1_1 || '-' || v_d2_1||'</b></div>';
    return_row_total_p1.PRICE2_SALE_SUM := 0;
    return_row_total_p1.PRICE2_BACK_SUM := 0;
    return_row_total_p1.PRICE2_SUM := 0;
    return_row_total_p1.DISC_SUM := 0;
    return_row_total_p1.COST_SUM := 0;
    return_row_total_p1.CNT := 0;
    return_row_total_p1.SALE_CNT := 0;
    return_row_total_p1.BACK_CNT := 0;
        
    return_row_total_p2.name := '<div width="100%" ><b>Всього'||' '|| v_d1_2 || '-' || v_d2_2||'</b></div>';
    return_row_total_p2.PRICE2_SALE_SUM := 0;
    return_row_total_p2.PRICE2_BACK_SUM := 0;
    return_row_total_p2.PRICE2_SUM := 0;
    return_row_total_p2.DISC_SUM := 0;
    return_row_total_p2.COST_SUM := 0;
    return_row_total_p2.CNT := 0;
    return_row_total_p2.SALE_CNT := 0;
    return_row_total_p2.BACK_CNT := 0;
    
    for c1 in (
        select distinct r.jur, j.name from(
            select distinct r.trpoint, t.name, t.jur from(
                select distinct trpoint from mv_sale1
                where dat between d1_1 and d2_1 
                    union all
                select distinct trpoint from mv_sale1
                where dat between d1_2 and d2_2
            ) r
            left join trpoint t on t.id=r.trpoint 
            where t.report_tp = 1
        ) r
        left join jur j on j.id=r.jur
    )loop
        -- виводим заголовок по юр. лицю
        return_row_p1:= null;
        return_row_p1.name := '<b>'||c1.name||'</b>';
        pipe row(return_row_p1);
        return_row_p1:= null;
        
        return_row_jur_p1.name := '<div width="100%" align="right"><b>Сума по юр. лицю'||' '|| v_d1_1 || '-' || v_d2_1||'</b></div>';
        return_row_jur_p1.PRICE2_SALE_SUM := 0;
        return_row_jur_p1.PRICE2_BACK_SUM := 0;
        return_row_jur_p1.PRICE2_SUM := 0;
        return_row_jur_p1.DISC_SUM := 0;
        return_row_jur_p1.COST_SUM := 0;
        return_row_jur_p1.CNT := 0;
        return_row_jur_p1.SALE_CNT := 0;
        return_row_jur_p1.BACK_CNT := 0;
        
        return_row_jur_p2.name := '<div width="100%" align="right"><b>Сума по юр. лицю'||' '|| v_d1_2 || '-' || v_d2_2||'</b></div>';
        return_row_jur_p2.PRICE2_SALE_SUM := 0;
        return_row_jur_p2.PRICE2_BACK_SUM := 0;
        return_row_jur_p2.PRICE2_SUM := 0;
        return_row_jur_p2.DISC_SUM := 0;
        return_row_jur_p2.COST_SUM := 0;
        return_row_jur_p2.CNT := 0;
        return_row_jur_p2.SALE_CNT := 0;
        return_row_jur_p2.BACK_CNT := 0;
        
        for c2 in (
            select distinct r.trpoint, t.name from(
                select distinct trpoint from mv_sale1
                where dat between d1_1 and d2_1 and sjur=c1.jur and part = 1 and isinet=1 and inet_tp=1
                    union all
                select distinct trpoint from mv_sale1
                where dat between d1_2 and d2_2 and sjur=c1.jur and part = 1 and isinet=1 and inet_tp=1
            ) r
            left join trpoint t on t.id=r.trpoint
            where t.report_tp = 1
        )loop  
            for c3 in (            
                SELECT t.id,
                (v_d1_1) dat1,
                (v_d2_1) dat2,
                t.name tt_name,
                case
                    when nvl(v_add_period,0) = 1 then
                        t.name ||' '|| v_d1_1 || '-' || v_d2_1
                    else
                        t.name
                end name,
                j.name jur,
                SUM(price) price_sum, 
                SUM(decode(r.tp,1,price2,0)) price2_sale_sum,
                SUM(decode(r.tp,2,abs(price2),0)) price2_back_sum,
                round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,
                SUM(price2) price2_sum,
                SUM(cost) cost_sum,
                SUM(cnt) cnt,
                (SUM(price) - SUM(cost)) disc_sum,
                case when SUM(price) != 0 then
                ROUND(SUM(cost)/SUM(price)*100,2)
                else 0 end pr_disc,
                case when SUM(SUM(price)) over() != 0 then
                ROUND(SUM(price)/SUM(SUM(price)) over()*100,2) 
                else 0 end pr,
                case when sum(sum(cnt)) over() !=0 then
                    sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt
                FROM
                (
                  SELECT trpoint,price,price2,cost,cnt,tp
                  FROM mv_sale1
                  WHERE dat BETWEEN v_d1_1 AND v_d2_1 and part = 1 and isinet=1 and inet_tp=1 and (V_WITHOUT_PKG is null or brand != 66) and trpoint = c2.trpoint
                ) r
                right JOIN trpoint t ON  r.trpoint = t.id
                left join jur j on t.jur=j.id
                where t.report_tp = 1
                GROUP BY t.id,t.name,j.name,(v_d1_1),(v_d2_1)
                having SUM(cnt) != 0
            )loop
                l_url := APEX_UTIL.PREPARE_URL(
                    p_url => 'f?p=104:43:'||v('SESSION')||'::NO:RP,43:P43_TRPOINT,P43_D1,P43_D2:'||c2.trpoint||','||v_d1_1||','||v_d2_1,
                    p_checksum_type => 'SESSION'
                );
                
                if nvl(v_add_period,0) = 1 then
                    return_row_p1.name := '<a href="'||l_url||'">'||c2.name||' '||v_d1_1||'-'||v_d2_1||'</a>';
                else
                    return_row_p1.name := '<a href="'||l_url||'">'||c2.name||'</a>';
                end if;
                
                return_row_p1.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                return_row_p1.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                return_row_p1.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                return_row_p1.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                return_row_p1.PR := to_char(c3.PR,'999G999G999G999G990D00');
                return_row_p1.PR_CNT := to_char(c3.PR_CNT,'999G999G999G999G990D00');
                return_row_p1.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                return_row_p1.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                return_row_p1.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                
                is_p1 := 1;
            end loop;
            
            if is_p1 != 1 then      
                l_url := APEX_UTIL.PREPARE_URL(
                    p_url => 'f?p=104:43:'||v('SESSION')||'::NO:RP,43:P43_TRPOINT,P43_D1,P43_D2:'||c2.trpoint||','||v_d1_1||','||v_d2_1,
                    p_checksum_type => 'SESSION'
                );
                            
                return_row_p1.name := '<a href="'||l_url||'">'||c2.name||' '||v_d1_1||'-'||v_d2_1||'</a>';
                return_row_p1.PRICE2_SALE_SUM := 0;
                return_row_p1.PRICE2_BACK_SUM := 0;
                return_row_p1.PROC_BACK_SUM := 0;
                return_row_p1.PRICE2_SUM := 0;
                return_row_p1.DISC_SUM := 0;
                return_row_p1.COST_SUM := 0;
                return_row_p1.PR_DISC := 0;
                return_row_p1.PR := 0;
                return_row_p1.PR_CNT := 0;
                return_row_p1.CNT := 0;
                return_row_p1.SALE_CNT := 0;
                return_row_p1.BACK_CNT := 0;
            end if;
            
            return_row_jur_p1.PRICE2_SALE_SUM := to_char(to_number(return_row_jur_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.PRICE2_BACK_SUM := to_char(to_number(return_row_jur_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.PRICE2_SUM := to_char(to_number(return_row_jur_p1.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.DISC_SUM := to_char(to_number(return_row_jur_p1.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.COST_SUM := to_char(to_number(return_row_jur_p1.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.CNT := to_char(to_number(return_row_jur_p1.CNT,'999G999G999G999G990D00') + to_number(return_row_p1.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.SALE_CNT := to_char(to_number(return_row_jur_p1.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.BACK_CNT := to_char(to_number(return_row_jur_p1.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            
            if nvl(v_add_period,0) = 1 then
                for c3 in (     
                    -- період 2 --
                    SELECT t.id, 
                    (v_d1_2) dat1,
                    (v_d2_2) dat2,
                    t.name tt_name,
                    case
                        when nvl(v_add_period,0) = 1 then
                            t.name ||' '|| v_d1_2 || '-' || v_d2_2
                        else
                            t.name
                    end name, 
                    j.name jur,
                    SUM(price) price_sum, 
                    SUM(decode(r.tp,1,price2,0)) price2_sale_sum,
                    SUM(decode(r.tp,2,abs(price2),0)) price2_back_sum,
                    round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                    case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                    case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,
                    SUM(price2) price2_sum,
                    SUM(cost) cost_sum,
                    SUM(cnt) cnt,
                    (SUM(price) - SUM(cost)) disc_sum,
                    case when SUM(price) != 0 then
                    ROUND(SUM(cost)/SUM(price)*100,2)
                    else 0 end pr_disc,
                    case when SUM(SUM(price)) over() != 0 then
                    ROUND(SUM(price)/SUM(SUM(price)) over()*100,2) 
                    else 0 end pr,
                    case when sum(sum(cnt)) over() !=0 then
                    sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt
                    FROM
                      (
                      SELECT trpoint,price,price2,cost,cnt,tp
                      FROM mv_sale1
                      WHERE dat BETWEEN v_d1_2 AND v_d2_2 and part = 1 and isinet=1 and inet_tp=1 and (V_WITHOUT_PKG is null or brand != 66) and trpoint = c2.trpoint
                    ) r
                    right JOIN trpoint t ON  r.trpoint = t.id
                    left join jur j on t.jur=j.id
                    where t.report_tp = 1
                    GROUP BY t.id,t.name,j.name,(v_d1_2),(v_d2_2)
                    having SUM(cnt) != 0
                )loop      
                    l_url := APEX_UTIL.PREPARE_URL(
                        p_url => 'f?p=104:43:'||v('SESSION')||'::NO:RP,43:P43_TRPOINT,P43_D1,P43_D2:'||c2.trpoint||','||v_d1_2||','||v_d2_2,
                        p_checksum_type => 'SESSION'
                    );
                    
                    if nvl(v_add_period,0) = 1 then
                        return_row_p2.name := '<a href="'||l_url||'">'||c2.name||' '||v_d1_2||'-'||v_d2_2||'</a>';
                    else
                        return_row_p2.name := '<a href="'||l_url||'">'||c2.name||'</a>';
                    end if;
                    
                    return_row_p2.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                    return_row_p2.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                    return_row_p2.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                    return_row_p2.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                    return_row_p2.PR := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.PR_CNT := to_char(c3.PR_CNT,'999G999G999G999G990D00');
                    return_row_p2.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                    return_row_p2.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                    return_row_p2.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                    
                    is_p2 := 1;
                end loop; 
                
                if is_p2 != 1 then      
                    l_url := APEX_UTIL.PREPARE_URL(
                        p_url => 'f?p=104:43:'||v('SESSION')||'::NO:RP,43:P43_TRPOINT,P43_D1,P43_D2:'||c2.trpoint||','||v_d1_2||','||v_d2_2,
                        p_checksum_type => 'SESSION'
                    );
                                
                    return_row_p2.name := '<a href="'||l_url||'">'||c2.name||' '||v_d1_2||'-'||v_d2_2||'</a>';
                    return_row_p2.PRICE2_SALE_SUM := 0;
                    return_row_p2.PRICE2_BACK_SUM := 0;
                    return_row_p2.PROC_BACK_SUM := 0;
                    return_row_p2.PRICE2_SUM := 0;
                    return_row_p2.DISC_SUM := 0;
                    return_row_p2.COST_SUM := 0;
                    return_row_p2.PR_DISC := 0;
                    return_row_p2.PR := 0;
                    return_row_p2.PR_CNT := 0;
                    return_row_p2.CNT := 0;
                    return_row_p2.SALE_CNT := 0;
                    return_row_p2.BACK_CNT := 0;
                end if;
                
                return_row_jur_p2.PRICE2_SALE_SUM := to_char(to_number(return_row_jur_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.PRICE2_BACK_SUM := to_char(to_number(return_row_jur_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.PRICE2_SUM := to_char(to_number(return_row_jur_p2.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.DISC_SUM := to_char(to_number(return_row_jur_p2.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.COST_SUM := to_char(to_number(return_row_jur_p2.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.CNT := to_char(to_number(return_row_jur_p2.CNT,'999G999G999G999G990D00') + to_number(return_row_p2.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.SALE_CNT := to_char(to_number(return_row_jur_p2.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.BACK_CNT := to_char(to_number(return_row_jur_p2.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                
     
                return_row_diff.NAME :='<div width="100%" align="right"><b>Різниця</b></div>'; 
                return_row_diff.PRICE2_SALE_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PROC_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PROC_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PROC_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.DISC_SUM := '<b>'||to_char(round(to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.COST_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_DISC := '<b>'||to_char(round(to_number(return_row_p2.PR_DISC,'999G999G999G999G990D00') - to_number(return_row_p1.PR_DISC,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR :=  '<b>'||to_char(round(to_number(return_row_p2.PR,'999G999G999G999G990D00') - to_number(return_row_p1.PR,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_CNT :=  '<b>'||to_char(round(to_number(return_row_p2.PR_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.PR_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.CNT := '<b>'||to_char(round(to_number(return_row_p2.cnt,'999G999G999G999G990D00') - to_number(return_row_p1.cnt,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.SALE_CNT := '<b>'||to_char(round(to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.BACK_CNT := '<b>'||to_char(round(to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                    
                pipe row(return_row_p1);
                pipe row(return_row_p2);
                pipe row(return_row_diff);
            else
                pipe row(return_row_p1);
            end if;
            
            return_row_p1 := null;
            return_row_p2 := null;
            return_row_diff := null;  
            
            is_p1 := 0;
            is_p2 := 0;
        end loop;
              
        return_row_total_p1.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.PRICE2_SUM := to_char(to_number(return_row_total_p1.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.DISC_SUM := to_char(to_number(return_row_total_p1.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.COST_SUM := to_char(to_number(return_row_total_p1.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.CNT := to_char(to_number(return_row_total_p1.CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p1.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.SALE_CNT := to_char(to_number(return_row_total_p1.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p1.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.BACK_CNT := to_char(to_number(return_row_total_p1.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p1.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        
        if nvl(v_add_period,0) = 1 then
            return_row_total_p2.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.PRICE2_SUM := to_char(to_number(return_row_total_p2.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.DISC_SUM := to_char(to_number(return_row_total_p2.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.COST_SUM := to_char(to_number(return_row_total_p2.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.CNT := to_char(to_number(return_row_total_p2.CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p2.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.SALE_CNT := to_char(to_number(return_row_total_p2.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p2.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.BACK_CNT := to_char(to_number(return_row_total_p2.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p2.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');    
        end if;
        
        return_row_jur_p1.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_jur_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_jur_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
        return_row_jur_p1.PRICE2_SALE_SUM := '<b>'||return_row_jur_p1.PRICE2_SALE_SUM ||'</b>';
        return_row_jur_p1.PRICE2_BACK_SUM := '<b>'||return_row_jur_p1.PRICE2_BACK_SUM ||'</b>';
        return_row_jur_p1.PRICE2_SUM := '<b>'||return_row_jur_p1.PRICE2_SUM ||'</b>';
        return_row_jur_p1.DISC_SUM := '<b>'||return_row_jur_p1.DISC_SUM ||'</b>';
        return_row_jur_p1.COST_SUM := '<b>'||return_row_jur_p1.COST_SUM ||'</b>';
        return_row_jur_p1.CNT := '<b>'||return_row_jur_p1.CNT ||'</b>';
        return_row_jur_p1.SALE_CNT := '<b>'||return_row_jur_p1.SALE_CNT ||'</b>';
        return_row_jur_p1.BACK_CNT := '<b>'||return_row_jur_p1.BACK_CNT ||'</b>';
        
        if nvl(v_add_period,0) = 1 then
            return_row_jur_p2.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_jur_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_jur_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
            return_row_jur_p2.PRICE2_SALE_SUM := '<b>'||return_row_jur_p2.PRICE2_SALE_SUM ||'</b>';
            return_row_jur_p2.PRICE2_BACK_SUM := '<b>'||return_row_jur_p2.PRICE2_BACK_SUM ||'</b>';
            return_row_jur_p2.PRICE2_SUM := '<b>'||return_row_jur_p2.PRICE2_SUM ||'</b>';
            return_row_jur_p2.DISC_SUM := '<b>'||return_row_jur_p2.DISC_SUM ||'</b>';
            return_row_jur_p2.COST_SUM := '<b>'||return_row_jur_p2.COST_SUM ||'</b>';
            return_row_jur_p2.CNT := '<b>'||return_row_jur_p2.CNT ||'</b>';
            return_row_jur_p2.SALE_CNT := '<b>'||return_row_jur_p2.SALE_CNT ||'</b>';
            return_row_jur_p2.BACK_CNT := '<b>'||return_row_jur_p2.BACK_CNT ||'</b>';
        end if;  
        
        pipe row(return_row_jur_p1);
        if nvl(v_add_period,0) = 1 then
            pipe row(return_row_jur_p2);
        end if;
    end loop;
    
    return_row_total_p1.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
    return_row_total_p1.PRICE2_SALE_SUM := '<b>'||return_row_total_p1.PRICE2_SALE_SUM ||'</b>';
    return_row_total_p1.PRICE2_BACK_SUM := '<b>'||return_row_total_p1.PRICE2_BACK_SUM ||'</b>';
    return_row_total_p1.PRICE2_SUM := '<b>'||return_row_total_p1.PRICE2_SUM ||'</b>';
    return_row_total_p1.DISC_SUM := '<b>'||return_row_total_p1.DISC_SUM ||'</b>';
    return_row_total_p1.COST_SUM := '<b>'||return_row_total_p1.COST_SUM ||'</b>';
    return_row_total_p1.CNT := '<b>'||return_row_total_p1.CNT ||'</b>';
    return_row_total_p1.SALE_CNT := '<b>'||return_row_total_p1.SALE_CNT ||'</b>';
    return_row_total_p1.BACK_CNT := '<b>'||return_row_total_p1.BACK_CNT ||'</b>';
        
    if nvl(v_add_period,0) = 1 then
        return_row_total_p2.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
        return_row_total_p2.PRICE2_SALE_SUM := '<b>'||return_row_total_p2.PRICE2_SALE_SUM ||'</b>';
        return_row_total_p2.PRICE2_BACK_SUM := '<b>'||return_row_total_p2.PRICE2_BACK_SUM ||'</b>';
        return_row_total_p2.PRICE2_SUM := '<b>'||return_row_total_p2.PRICE2_SUM ||'</b>';
        return_row_total_p2.DISC_SUM := '<b>'||return_row_total_p2.DISC_SUM ||'</b>';
        return_row_total_p2.COST_SUM := '<b>'||return_row_total_p2.COST_SUM ||'</b>';
        return_row_total_p2.CNT := '<b>'||return_row_total_p2.CNT ||'</b>';
        return_row_total_p2.SALE_CNT := '<b>'||return_row_total_p2.SALE_CNT ||'</b>';
        return_row_total_p2.BACK_CNT := '<b>'||return_row_total_p2.BACK_CNT ||'</b>';
    end if;   
    
    pipe row(return_row_total_p1);
    if nvl(v_add_period,0) = 1 then   
        pipe row(return_row_total_p2);
    end if;
end sales_by_shops;
-------------------------

-- продажі -> по брендам
function sales_by_brand(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined
as
    v_d1_1 date;
    v_d2_1 date;
    v_d1_2 date;
    v_d2_2 date;
    
    return_row_p1 sales_by_shops_r_char;
    return_row_p2 sales_by_shops_r_char;
    return_row_diff sales_by_shops_r_char;
    return_row_total_p1 sales_by_shops_r_char;
    return_row_total_p2 sales_by_shops_r_char;
    
    is_p1 number := 0;
    is_p2 number := 0;
    l_url varchar2(4000);
begin

    if d1_1 > d1_2 then
        v_d1_1 := d1_2;
        v_d2_1 := d2_2;
        
        v_d1_2 := d1_1;
        v_d2_2 := d2_1;
    else
        v_d1_2 := d1_2;
        v_d2_2 := d2_2;
        
        v_d1_1 := d1_1;
        v_d2_1 := d2_1;
    end if;
    
    return_row_total_p1.name := '<div width="100%" ><b>Всього'||' '|| v_d1_1 || '-' || v_d2_1||'</b></div>';
    return_row_total_p1.PRICE2_SALE_SUM := 0;
    return_row_total_p1.PRICE2_BACK_SUM := 0;
    return_row_total_p1.PRICE2_SUM := 0;
    return_row_total_p1.DISC_SUM := 0;
    return_row_total_p1.COST_SUM := 0;
    return_row_total_p1.CNT := 0;
    return_row_total_p1.SALE_CNT := 0;
    return_row_total_p1.BACK_CNT := 0;
        
    return_row_total_p2.name := '<div width="100%" ><b>Всього'||' '|| v_d1_2 || '-' || v_d2_2||'</b></div>';
    return_row_total_p2.PRICE2_SALE_SUM := 0;
    return_row_total_p2.PRICE2_BACK_SUM := 0;
    return_row_total_p2.PRICE2_SUM := 0;
    return_row_total_p2.DISC_SUM := 0;
    return_row_total_p2.COST_SUM := 0;
    return_row_total_p2.CNT := 0;
    return_row_total_p2.SALE_CNT := 0;
    return_row_total_p2.BACK_CNT := 0;
    
    for c1 in (
        select distinct r.brand, b.name from(
            select distinct brand from mv_sale1
            where dat between d1_1 and d2_1 and part = 1  and isinet=1 and inet_tp=1
            and trpoint in (select id from trpoint where report_tp = 1 )
                union all
            select distinct brand from mv_sale1
            where dat between d1_2 and d2_2 and part = 1  and isinet=1 and inet_tp=1
            and trpoint in (select id from trpoint where report_tp = 1 )
        ) r
        left join brands b on r.brand= b.id
    )loop
        if nvl(v_add_period,0) = 1 then
            -- виводим заголовок по юр. лицю
            return_row_p1:= null;
            return_row_p1.name := '<b>'||c1.name||'</b>';
            pipe row(return_row_p1);
            return_row_p1:= null;
        end if;
          
            for c3 in (            
                SELECT 
                (v_d1_1) dat1,
                (v_d2_1) dat2,
                t.id,
                case
                    when nvl(v_add_period,0) = 1 then
                        t.name ||' '|| v_d1_1 || '-' || v_d2_1
                    else
                        t.name
                end name, 
                t.name brand,
                SUM(price) PRICE2_SUM, 
                SUM(cost) cost_sum,
                SUM(cnt) cnt,
                (SUM(price) - SUM(cost)) disc_sum,
                case when SUM(price) != 0 then
                ROUND(SUM(cost)/SUM(price)*100,2)
                else 0 end pr_disc,
                case when sum(sum(cnt)) over() !=0 then
                sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt,
                case when sum(sum(price)) over() !=0 then
                sum(price)/sum(sum(price)) over()*100 else 0 end pr,
                round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                sum(price2_back_sum) price2_back_sum,
                case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,    
                sum(price2_sale_sum) price2_sale_sum
                from (
                    SELECT brand,sum(price) price,sum(price2) price2,sum(price2val) price2val, tp,
                    sum(cost) cost,sum(cost_buh) cost_buh,sum(cnt) cnt,0 cntp,0 rst,
                    SUM(decode(m.tp,2,abs(price2),0)) price2_back_sum,
                    SUM(decode(m.tp,1,price2,0)) price2_sale_sum
                    FROM mv_sale1 m
                    WHERE dat BETWEEN v_d1_1 AND v_d2_1
                    and part = 1  and isinet=1 and inet_tp=1 and brand=c1.brand
                    and (v_without_pkg is null or brand != 66)
                    and trpoint in (select id from trpoint where report_tp = 1 ) 
                    group by brand,tp
                    having sum(cnt) !=0 
                ) r
                right JOIN brands t ON  r.brand = t.id
                GROUP BY t.id,t.name,(v_d1_1), (v_d2_1),t.name
                having sum(cnt) !=0
            )loop
                
                if nvl(v_add_period,0) = 1 then
                    return_row_p1.name := v_d1_1||'-'||v_d2_1;
                else
                    return_row_p1.name := c1.name;
                end if;
                
                return_row_p1.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                return_row_p1.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                return_row_p1.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                return_row_p1.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                return_row_p1.PR := to_char(c3.PR,'999G999G999G999G990D00');
                return_row_p1.PR_CNT := to_char(c3.PR_CNT,'999G999G999G999G990D00');
                return_row_p1.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                return_row_p1.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                return_row_p1.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                
                is_p1 := 1;
            end loop;
            
            if is_p1 != 1 then      
                return_row_p1.name := v_d1_1||'-'||v_d2_1;
                return_row_p1.PRICE2_SALE_SUM := 0;
                return_row_p1.PRICE2_BACK_SUM := 0;
                return_row_p1.PROC_BACK_SUM := 0;
                return_row_p1.PRICE2_SUM := 0;
                return_row_p1.DISC_SUM := 0;
                return_row_p1.COST_SUM := 0;
                return_row_p1.PR_DISC := 0;
                return_row_p1.PR := 0;
                return_row_p1.PR_CNT := 0;
                return_row_p1.CNT := 0;
                return_row_p1.SALE_CNT := 0;
                return_row_p1.BACK_CNT := 0;
            end if;
            
            if nvl(v_add_period,0) = 1 then
                for c3 in (     
                    -- період 2 --
                    SELECT 
                    (v_d1_2) dat1,
                    (v_d2_2) dat2,
                    t.id,
                    case
                        when nvl(v_add_period,0) = 1 then
                            t.name ||' '|| v_d1_2 || '-' || v_d2_2
                        else
                            t.name
                    end name, 
                    t.name brand,
                    SUM(price) PRICE2_SUM, 
                    SUM(cost) cost_sum,
                    SUM(cnt) cnt,
                    (SUM(price) - SUM(cost)) disc_sum,
                    case when SUM(price) != 0 then
                    ROUND(SUM(cost)/SUM(price)*100,2)
                    else 0 end pr_disc,
                    case when sum(sum(cnt)) over() !=0 then
                    sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt,
                    case when sum(sum(price)) over() !=0 then
                    sum(price)/sum(sum(price)) over()*100 else 0 end pr,
                    round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                    sum(price2_back_sum) price2_back_sum,   
                    case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                    case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,  
                    sum(price2_sale_sum) price2_sale_sum
                    from (
                        SELECT brand,sum(price) price,sum(price2) price2,sum(price2val) price2val, tp,
                        sum(cost) cost,sum(cost_buh) cost_buh,sum(cnt) cnt,0 cntp,0 rst,
                        SUM(decode(m.tp,2,abs(price2),0)) price2_back_sum,
                        SUM(decode(m.tp,1,price2,0)) price2_sale_sum
                        FROM mv_sale1 m
                        WHERE dat BETWEEN v_d1_2 AND v_d2_2
                        and part = 1  and isinet=1 and inet_tp=1 and brand=c1.brand
                        and (v_without_pkg is null or brand != 66)
                        and trpoint in (select id from trpoint where report_tp = 1 ) 
                        group by brand,tp
                        having sum(cnt) !=0 
                    ) r
                    right JOIN brands t ON  r.brand = t.id
                    GROUP BY t.id,t.name,(v_d1_2), (v_d2_2),t.name
                    having sum(cnt) !=0
                )loop      
                    
                    if nvl(v_add_period,0) = 1 then
                        return_row_p2.name := v_d1_2||'-'||v_d2_2;
                    else
                        return_row_p2.name := c1.name;
                    end if;
                    
                    return_row_p2.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                    return_row_p2.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                    return_row_p2.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                    return_row_p2.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                    return_row_p2.PR := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.PR_CNT := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                    return_row_p2.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                    return_row_p2.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                    
                    is_p2 := 1;
                end loop; 
                
                if is_p2 != 1 then                             
                    return_row_p2.name := v_d1_2||'-'||v_d2_2;
                    return_row_p2.PRICE2_SALE_SUM := 0;
                    return_row_p2.PRICE2_BACK_SUM := 0;
                    return_row_p2.PROC_BACK_SUM := 0;
                    return_row_p2.PRICE2_SUM := 0;
                    return_row_p2.DISC_SUM := 0;
                    return_row_p2.COST_SUM := 0;
                    return_row_p2.PR_DISC := 0;
                    return_row_p2.PR := 0;
                    return_row_p2.PR_CNT := 0;
                    return_row_p2.CNT := 0;
                    return_row_p2.SALE_CNT := 0;
                    return_row_p2.BACK_CNT := 0;
                end if;
                   
                return_row_diff.NAME :='<div width="100%" align="right"><b>Різниця</b></div>'; 
                return_row_diff.PRICE2_SALE_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PROC_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PROC_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PROC_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.DISC_SUM := '<b>'||to_char(round(to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.COST_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_DISC := '<b>'||to_char(round(to_number(return_row_p2.PR_DISC,'999G999G999G999G990D00') - to_number(return_row_p1.PR_DISC,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR :=  '<b>'||to_char(round(to_number(return_row_p2.PR,'999G999G999G999G990D00') - to_number(return_row_p1.PR,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_CNT :=  '<b>'||to_char(round(to_number(return_row_p2.PR_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.PR_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.CNT := '<b>'||to_char(round(to_number(return_row_p2.cnt,'999G999G999G999G990D00') - to_number(return_row_p1.cnt,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.SALE_CNT := '<b>'||to_char(round(to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.BACK_CNT := '<b>'||to_char(round(to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                    
                pipe row(return_row_p1);
                pipe row(return_row_p2);
                pipe row(return_row_diff);
            else
                pipe row(return_row_p1);
            end if;
            
            return_row_total_p1.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.PRICE2_SUM := to_char(to_number(return_row_total_p1.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.DISC_SUM := to_char(to_number(return_row_total_p1.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.COST_SUM := to_char(to_number(return_row_total_p1.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.CNT := to_char(to_number(return_row_total_p1.CNT,'999G999G999G999G990D00') + to_number(return_row_p1.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.SALE_CNT := to_char(to_number(return_row_total_p1.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.BACK_CNT := to_char(to_number(return_row_total_p1.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            
            if nvl(v_add_period,0) = 1 then
                return_row_total_p2.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.PRICE2_SUM := to_char(to_number(return_row_total_p2.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.DISC_SUM := to_char(to_number(return_row_total_p2.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.COST_SUM := to_char(to_number(return_row_total_p2.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.CNT := to_char(to_number(return_row_total_p2.CNT,'999G999G999G999G990D00') + to_number(return_row_p2.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.SALE_CNT := to_char(to_number(return_row_total_p2.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.BACK_CNT := to_char(to_number(return_row_total_p2.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');    
            end if;
            
            return_row_p1 := null;
            return_row_p2 := null;
            return_row_diff := null;  
            
            is_p1 := 0;
            is_p2 := 0;
        end loop;
              
    return_row_total_p1.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
    return_row_total_p1.PRICE2_SALE_SUM := '<b>'||return_row_total_p1.PRICE2_SALE_SUM ||'</b>';
    return_row_total_p1.PRICE2_BACK_SUM := '<b>'||return_row_total_p1.PRICE2_BACK_SUM ||'</b>';
    return_row_total_p1.PRICE2_SUM := '<b>'||return_row_total_p1.PRICE2_SUM ||'</b>';
    return_row_total_p1.DISC_SUM := '<b>'||return_row_total_p1.DISC_SUM ||'</b>';
    return_row_total_p1.COST_SUM := '<b>'||return_row_total_p1.COST_SUM ||'</b>';
    return_row_total_p1.CNT := '<b>'||return_row_total_p1.CNT ||'</b>';
    return_row_total_p1.SALE_CNT := '<b>'||return_row_total_p1.SALE_CNT ||'</b>';
    return_row_total_p1.BACK_CNT := '<b>'||return_row_total_p1.BACK_CNT ||'</b>';
        
    if nvl(v_add_period,0) = 1 then
        return_row_total_p2.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
        return_row_total_p2.PRICE2_SALE_SUM := '<b>'||return_row_total_p2.PRICE2_SALE_SUM ||'</b>';
        return_row_total_p2.PRICE2_BACK_SUM := '<b>'||return_row_total_p2.PRICE2_BACK_SUM ||'</b>';
        return_row_total_p2.PRICE2_SUM := '<b>'||return_row_total_p2.PRICE2_SUM ||'</b>';
        return_row_total_p2.DISC_SUM := '<b>'||return_row_total_p2.DISC_SUM ||'</b>';
        return_row_total_p2.COST_SUM := '<b>'||return_row_total_p2.COST_SUM ||'</b>';
        return_row_total_p2.CNT := '<b>'||return_row_total_p2.CNT ||'</b>';
        return_row_total_p2.SALE_CNT := '<b>'||return_row_total_p2.SALE_CNT ||'</b>';
        return_row_total_p2.BACK_CNT := '<b>'||return_row_total_p2.BACK_CNT ||'</b>';
    end if;   
    
    pipe row(return_row_total_p1);
    if nvl(v_add_period,0) = 1 then   
        pipe row(return_row_total_p2);
    end if;
end sales_by_brand;
-------------------------

-- продажі -> по групам
function sales_by_group(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined
as
    v_d1_1 date;
    v_d2_1 date;
    v_d1_2 date;
    v_d2_2 date;
    
    return_row_p1 sales_by_shops_r_char;
    return_row_p2 sales_by_shops_r_char;
    return_row_diff sales_by_shops_r_char;
    return_row_total_p1 sales_by_shops_r_char;
    return_row_total_p2 sales_by_shops_r_char;
    
    is_p1 number := 0;
    is_p2 number := 0;
    l_url varchar2(4000);
begin
    if d1_1 > d1_2 then
        v_d1_1 := d1_2;
        v_d2_1 := d2_2;
        
        v_d1_2 := d1_1;
        v_d2_2 := d2_1;
    else
        v_d1_2 := d1_2;
        v_d2_2 := d2_2;
        
        v_d1_1 := d1_1;
        v_d2_1 := d2_1;
    end if;
    
    return_row_total_p1.name := '<div width="100%" ><b>Всього'||' '|| v_d1_1 || '-' || v_d2_1||'</b></div>';
    return_row_total_p1.PRICE2_SALE_SUM := 0;
    return_row_total_p1.PRICE2_BACK_SUM := 0;
    return_row_total_p1.PRICE2_SUM := 0;
    return_row_total_p1.DISC_SUM := 0;
    return_row_total_p1.COST_SUM := 0;
    return_row_total_p1.CNT := 0;
    return_row_total_p1.SALE_CNT := 0;
    return_row_total_p1.BACK_CNT := 0;
        
    return_row_total_p2.name := '<div width="100%" ><b>Всього'||' '|| v_d1_2 || '-' || v_d2_2||'</b></div>';
    return_row_total_p2.PRICE2_SALE_SUM := 0;
    return_row_total_p2.PRICE2_BACK_SUM := 0;
    return_row_total_p2.PRICE2_SUM := 0;
    return_row_total_p2.DISC_SUM := 0;
    return_row_total_p2.COST_SUM := 0;
    return_row_total_p2.CNT := 0;
    return_row_total_p2.SALE_CNT := 0;
    return_row_total_p2.BACK_CNT := 0;
    
    for c1 in (
        select distinct r.grp2, b.name from(
            select distinct grp2 from mv_sale1
            where dat between d1_1 and d2_1 and part = 1  and isinet=1 and inet_tp=1 and (v_without_pkg is null or brand != 66)
            and trpoint in (select id from trpoint where report_tp = 1 )
                union all
            select distinct grp2 from mv_sale1
            where dat between d1_2 and d2_2 and part = 1  and isinet=1 and inet_tp=1 and (v_without_pkg is null or brand != 66)
            and trpoint in (select id from trpoint where report_tp = 1 )
        ) r
        left join grp2 b on r.grp2= b.id
    )loop
        if nvl(v_add_period,0) = 1 then
            -- виводим заголовок по юр. лицю
            return_row_p1:= null;
            return_row_p1.name := '<b>'||c1.name||'</b>';
            pipe row(return_row_p1);
            return_row_p1:= null;
        end if;
            for c3 in (            
                SELECT 
                (v_d1_1) dat1,
                (v_d2_1) dat2,
                t.id,
                case
                    when nvl(v_add_period,0) = 1 then
                        t.name ||' '|| v_d1_1 || '-' || v_d2_1
                    else
                        t.name
                end name, 
                t.name grp2,
                SUM(price) PRICE2_SUM, 
                SUM(cost) cost_sum,
                SUM(cnt) cnt,
                (SUM(price) - SUM(cost)) disc_sum,
                case when SUM(price) != 0 then
                ROUND(SUM(cost)/SUM(price)*100,2)
                else 0 end pr_disc,
                case when sum(sum(cnt)) over() !=0 then
                sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt,
                case when sum(sum(price)) over() !=0 then
                sum(price)/sum(sum(price)) over()*100 else 0 end pr,
                round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                sum(price2_back_sum) price2_back_sum,
                case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,    
                sum(price2_sale_sum) price2_sale_sum
                from (
                    SELECT grp2,sum(price) price,sum(price2) price2,sum(price2val) price2val, tp,
                    sum(cost) cost,sum(cost_buh) cost_buh,sum(cnt) cnt,0 cntp,0 rst,
                    SUM(decode(m.tp,2,abs(price2),0)) price2_back_sum,
                    SUM(decode(m.tp,1,price2,0)) price2_sale_sum
                    FROM mv_sale1 m
                    WHERE dat BETWEEN v_d1_1 AND v_d2_1
                    and part = 1  and isinet=1 and inet_tp=1 and grp2=c1.grp2
                    and (v_without_pkg is null or brand != 66)
                    and trpoint in (select id from trpoint where report_tp = 1 ) 
                    group by grp2,tp
                    having sum(cnt) !=0 
                ) r
                right JOIN grp2 t ON  r.grp2 = t.id
                GROUP BY t.id,t.name,(v_d1_1), (v_d2_1),t.name
                having sum(cnt) !=0
            )loop
                
                if nvl(v_add_period,0) = 1 then
                    return_row_p1.name := v_d1_1||'-'||v_d2_1;
                else
                    return_row_p1.name := c1.name;
                end if;
                
                return_row_p1.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                return_row_p1.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                return_row_p1.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                return_row_p1.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                return_row_p1.PR := to_char(c3.PR,'999G999G999G999G990D00');
                return_row_p1.PR_CNT := to_char(c3.PR_CNT,'999G999G999G999G990D00');
                return_row_p1.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                return_row_p1.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                return_row_p1.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                
                is_p1 := 1;
            end loop;
            
            if is_p1 != 1 then      
                return_row_p1.name := v_d1_1||'-'||v_d2_1;
                return_row_p1.PRICE2_SALE_SUM := 0;
                return_row_p1.PRICE2_BACK_SUM := 0;
                return_row_p1.PROC_BACK_SUM := 0;
                return_row_p1.PRICE2_SUM := 0;
                return_row_p1.DISC_SUM := 0;
                return_row_p1.COST_SUM := 0;
                return_row_p1.PR_DISC := 0;
                return_row_p1.PR := 0;
                return_row_p1.PR_CNT := 0;
                return_row_p1.CNT := 0;
                return_row_p1.SALE_CNT := 0;
                return_row_p1.BACK_CNT := 0;
            end if;
            
            if nvl(v_add_period,0) = 1 then
                for c3 in (     
                    -- період 2 --
                    SELECT 
                    (v_d1_2) dat1,
                    (v_d2_2) dat2,
                    t.id,
                    case
                        when nvl(v_add_period,0) = 1 then
                            t.name ||' '|| v_d1_2 || '-' || v_d2_2
                        else
                            t.name
                    end name, 
                    t.name grp2,
                    SUM(price) PRICE2_SUM, 
                    SUM(cost) cost_sum,
                    SUM(cnt) cnt,
                    (SUM(price) - SUM(cost)) disc_sum,
                    case when SUM(price) != 0 then
                    ROUND(SUM(cost)/SUM(price)*100,2)
                    else 0 end pr_disc,
                    case when sum(sum(cnt)) over() !=0 then
                    sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt,
                    case when sum(sum(price)) over() !=0 then
                    sum(price)/sum(sum(price)) over()*100 else 0 end pr,
                    round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                    sum(price2_back_sum) price2_back_sum,   
                    case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                    case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,  
                    sum(price2_sale_sum) price2_sale_sum
                    from (
                        SELECT grp2,sum(price) price,sum(price2) price2,sum(price2val) price2val, tp,
                        sum(cost) cost,sum(cost_buh) cost_buh,sum(cnt) cnt,0 cntp,0 rst,
                        SUM(decode(m.tp,2,abs(price2),0)) price2_back_sum,
                        SUM(decode(m.tp,1,price2,0)) price2_sale_sum
                        FROM mv_sale1 m
                        WHERE dat BETWEEN v_d1_2 AND v_d2_2
                        and part = 1 and isinet=1 and inet_tp=1 and grp2=c1.grp2
                        and (v_without_pkg is null or brand != 66)
                        and trpoint in (select id from trpoint where report_tp = 1 ) 
                        group by grp2,tp
                        having sum(cnt) !=0 
                    ) r
                    right JOIN grp2 t ON  r.grp2 = t.id
                    GROUP BY t.id,t.name,(v_d1_2), (v_d2_2),t.name
                    having sum(cnt) !=0
                )loop      
                    
                    if nvl(v_add_period,0) = 1 then
                        return_row_p2.name := v_d1_2||'-'||v_d2_2;
                    else
                        return_row_p2.name := c1.name;
                    end if;
                    
                    return_row_p2.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                    return_row_p2.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                    return_row_p2.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                    return_row_p2.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                    return_row_p2.PR := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.PR_CNT := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                    return_row_p2.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                    return_row_p2.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                    
                    is_p2 := 1;
                end loop; 
                
                if is_p2 != 1 then                             
                    return_row_p2.name := v_d1_2||'-'||v_d2_2;
                    return_row_p2.PRICE2_SALE_SUM := 0;
                    return_row_p2.PRICE2_BACK_SUM := 0;
                    return_row_p2.PROC_BACK_SUM := 0;
                    return_row_p2.PRICE2_SUM := 0;
                    return_row_p2.DISC_SUM := 0;
                    return_row_p2.COST_SUM := 0;
                    return_row_p2.PR_DISC := 0;
                    return_row_p2.PR := 0;
                    return_row_p2.PR_CNT := 0;
                    return_row_p2.CNT := 0;
                    return_row_p2.SALE_CNT := 0;
                    return_row_p2.BACK_CNT := 0;
                end if;
                   
                return_row_diff.NAME :='<div width="100%" align="right"><b>Різниця</b></div>'; 
                return_row_diff.PRICE2_SALE_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PROC_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PROC_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PROC_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.DISC_SUM := '<b>'||to_char(round(to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.COST_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_DISC := '<b>'||to_char(round(to_number(return_row_p2.PR_DISC,'999G999G999G999G990D00') - to_number(return_row_p1.PR_DISC,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR :=  '<b>'||to_char(round(to_number(return_row_p2.PR,'999G999G999G999G990D00') - to_number(return_row_p1.PR,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_CNT :=  '<b>'||to_char(round(to_number(return_row_p2.PR_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.PR_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.CNT := '<b>'||to_char(round(to_number(return_row_p2.cnt,'999G999G999G999G990D00') - to_number(return_row_p1.cnt,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.SALE_CNT := '<b>'||to_char(round(to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.BACK_CNT := '<b>'||to_char(round(to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                    
                pipe row(return_row_p1);
                pipe row(return_row_p2);
                pipe row(return_row_diff);
            else
                pipe row(return_row_p1);
            end if;
            
            return_row_total_p1.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.PRICE2_SUM := to_char(to_number(return_row_total_p1.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.DISC_SUM := to_char(to_number(return_row_total_p1.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.COST_SUM := to_char(to_number(return_row_total_p1.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.CNT := to_char(to_number(return_row_total_p1.CNT,'999G999G999G999G990D00') + to_number(return_row_p1.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.SALE_CNT := to_char(to_number(return_row_total_p1.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.BACK_CNT := to_char(to_number(return_row_total_p1.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            
            if nvl(v_add_period,0) = 1 then
                return_row_total_p2.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.PRICE2_SUM := to_char(to_number(return_row_total_p2.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.DISC_SUM := to_char(to_number(return_row_total_p2.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.COST_SUM := to_char(to_number(return_row_total_p2.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.CNT := to_char(to_number(return_row_total_p2.CNT,'999G999G999G999G990D00') + to_number(return_row_p2.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.SALE_CNT := to_char(to_number(return_row_total_p2.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.BACK_CNT := to_char(to_number(return_row_total_p2.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');    
            end if;
            
            return_row_p1 := null;
            return_row_p2 := null;
            return_row_diff := null;  
            
            is_p1 := 0;
            is_p2 := 0;
        end loop;
              
    return_row_total_p1.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
    return_row_total_p1.PRICE2_SALE_SUM := '<b>'||return_row_total_p1.PRICE2_SALE_SUM ||'</b>';
    return_row_total_p1.PRICE2_BACK_SUM := '<b>'||return_row_total_p1.PRICE2_BACK_SUM ||'</b>';
    return_row_total_p1.PRICE2_SUM := '<b>'||return_row_total_p1.PRICE2_SUM ||'</b>';
    return_row_total_p1.DISC_SUM := '<b>'||return_row_total_p1.DISC_SUM ||'</b>';
    return_row_total_p1.COST_SUM := '<b>'||return_row_total_p1.COST_SUM ||'</b>';
    return_row_total_p1.CNT := '<b>'||return_row_total_p1.CNT ||'</b>';
    return_row_total_p1.SALE_CNT := '<b>'||return_row_total_p1.SALE_CNT ||'</b>';
    return_row_total_p1.BACK_CNT := '<b>'||return_row_total_p1.BACK_CNT ||'</b>';
        
    if nvl(v_add_period,0) = 1 then
        return_row_total_p2.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
        return_row_total_p2.PRICE2_SALE_SUM := '<b>'||return_row_total_p2.PRICE2_SALE_SUM ||'</b>';
        return_row_total_p2.PRICE2_BACK_SUM := '<b>'||return_row_total_p2.PRICE2_BACK_SUM ||'</b>';
        return_row_total_p2.PRICE2_SUM := '<b>'||return_row_total_p2.PRICE2_SUM ||'</b>';
        return_row_total_p2.DISC_SUM := '<b>'||return_row_total_p2.DISC_SUM ||'</b>';
        return_row_total_p2.COST_SUM := '<b>'||return_row_total_p2.COST_SUM ||'</b>';
        return_row_total_p2.CNT := '<b>'||return_row_total_p2.CNT ||'</b>';
        return_row_total_p2.SALE_CNT := '<b>'||return_row_total_p2.SALE_CNT ||'</b>';
        return_row_total_p2.BACK_CNT := '<b>'||return_row_total_p2.BACK_CNT ||'</b>';
    end if;   
    
    pipe row(return_row_total_p1);
    if nvl(v_add_period,0) = 1 then   
        pipe row(return_row_total_p2);
    end if;
end sales_by_group;
-------------------------

-- продажі -> по сезонам
function sales_by_szon(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined
as
    v_d1_1 date;
    v_d2_1 date;
    v_d1_2 date;
    v_d2_2 date;
    
    return_row_p1 sales_by_shops_r_char;
    return_row_p2 sales_by_shops_r_char;
    return_row_diff sales_by_shops_r_char;
    return_row_total_p1 sales_by_shops_r_char;
    return_row_total_p2 sales_by_shops_r_char;
    
    is_p1 number := 0;
    is_p2 number := 0;
    l_url varchar2(4000);
begin

    if d1_1 > d1_2 then
        v_d1_1 := d1_2;
        v_d2_1 := d2_2;
        
        v_d1_2 := d1_1;
        v_d2_2 := d2_1;
    else
        v_d1_2 := d1_2;
        v_d2_2 := d2_2;
        
        v_d1_1 := d1_1;
        v_d2_1 := d2_1;
    end if;
    
    return_row_total_p1.name := '<div width="100%" ><b>Всього'||' '|| v_d1_1 || '-' || v_d2_1||'</b></div>';
    return_row_total_p1.PRICE2_SALE_SUM := 0;
    return_row_total_p1.PRICE2_BACK_SUM := 0;
    return_row_total_p1.PRICE2_SUM := 0;
    return_row_total_p1.DISC_SUM := 0;
    return_row_total_p1.COST_SUM := 0;
    return_row_total_p1.CNT := 0;
    return_row_total_p1.SALE_CNT := 0;
    return_row_total_p1.BACK_CNT := 0;
        
    return_row_total_p2.name := '<div width="100%" ><b>Всього'||' '|| v_d1_2 || '-' || v_d2_2||'</b></div>';
    return_row_total_p2.PRICE2_SALE_SUM := 0;
    return_row_total_p2.PRICE2_BACK_SUM := 0;
    return_row_total_p2.PRICE2_SUM := 0;
    return_row_total_p2.DISC_SUM := 0;
    return_row_total_p2.COST_SUM := 0;
    return_row_total_p2.CNT := 0;
    return_row_total_p2.SALE_CNT := 0;
    return_row_total_p2.BACK_CNT := 0;
    
    for c1 in (
        select distinct r.szon, b.name, b.id from(
            select distinct szon from mv_sale1
            where dat between d1_1 and d2_1 and part = 1  and isinet=1 and inet_tp=1
            and trpoint in (select id from trpoint where report_tp = 1 )
                union all
            select distinct szon from mv_sale1
            where dat between d1_2 and d2_2 and part = 1  and isinet=1 and inet_tp=1
            and trpoint in (select id from trpoint where report_tp = 1 )
        ) r
        left join col b on r.szon= b.id
        order by b.id desc
    )loop
        if nvl(v_add_period,0) = 1 then
            -- виводим заголовок по юр. лицю
            return_row_p1:= null;
            return_row_p1.name := '<b>'||c1.name||'</b>';
            pipe row(return_row_p1);
            return_row_p1:= null;
        end if;
          
            for c3 in (            
                SELECT 
                (v_d1_1) dat1,
                (v_d2_1) dat2,
                t.id,
                case
                    when nvl(v_add_period,0) = 1 then
                        t.name ||' '|| v_d1_1 || '-' || v_d2_1
                    else
                        t.name
                end name, 
                t.name brand,
                SUM(price) PRICE2_SUM, 
                SUM(cost) cost_sum,
                SUM(cnt) cnt,
                (SUM(price) - SUM(cost)) disc_sum,
                case when SUM(price) != 0 then
                ROUND(SUM(cost)/SUM(price)*100,2)
                else 0 end pr_disc,
                case when sum(sum(cnt)) over() !=0 then
                sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt,
                case when sum(sum(price)) over() !=0 then
                sum(price)/sum(sum(price)) over()*100 else 0 end pr,
                round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                sum(price2_back_sum) price2_back_sum,
                case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,    
                sum(price2_sale_sum) price2_sale_sum
                from (
                    SELECT szon,sum(price) price,sum(price2) price2,sum(price2val) price2val, tp,
                    sum(cost) cost,sum(cost_buh) cost_buh,sum(cnt) cnt,0 cntp,0 rst,
                    SUM(decode(m.tp,2,abs(price2),0)) price2_back_sum,
                    SUM(decode(m.tp,1,price2,0)) price2_sale_sum
                    FROM mv_sale1 m
                    WHERE dat BETWEEN v_d1_1 AND v_d2_1
                    and part = 1  and isinet=1 and inet_tp=1 and szon=c1.szon
                    and (v_without_pkg is null or brand != 66)
                    and trpoint in (select id from trpoint where report_tp = 1 ) 
                    group by szon,tp
                    having sum(cnt) !=0 
                ) r
                right JOIN col t ON  r.szon = t.id
                GROUP BY t.id,t.name,(v_d1_1), (v_d2_1),t.name
                having sum(cnt) !=0
            )loop
                
                if nvl(v_add_period,0) = 1 then
                    return_row_p1.name := v_d1_1||'-'||v_d2_1;
                else
                    return_row_p1.name := c1.name;
                end if;
                
                return_row_p1.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                return_row_p1.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                return_row_p1.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                return_row_p1.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                return_row_p1.PR := to_char(c3.PR,'999G999G999G999G990D00');
                return_row_p1.PR_CNT := to_char(c3.PR_CNT,'999G999G999G999G990D00');
                return_row_p1.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                return_row_p1.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                return_row_p1.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                
                is_p1 := 1;
            end loop;
            
            if is_p1 != 1 then      
                return_row_p1.name := v_d1_1||'-'||v_d2_1;
                return_row_p1.PRICE2_SALE_SUM := 0;
                return_row_p1.PRICE2_BACK_SUM := 0;
                return_row_p1.PROC_BACK_SUM := 0;
                return_row_p1.PRICE2_SUM := 0;
                return_row_p1.DISC_SUM := 0;
                return_row_p1.COST_SUM := 0;
                return_row_p1.PR_DISC := 0;
                return_row_p1.PR := 0;
                return_row_p1.PR_CNT := 0;
                return_row_p1.CNT := 0;
                return_row_p1.SALE_CNT := 0;
                return_row_p1.BACK_CNT := 0;
            end if;
            
            if nvl(v_add_period,0) = 1 then
                for c3 in (     
                    -- період 2 --
                    SELECT 
                    (v_d1_2) dat1,
                    (v_d2_2) dat2,
                    t.id,
                    case
                        when nvl(v_add_period,0) = 1 then
                            t.name ||' '|| v_d1_2 || '-' || v_d2_2
                        else
                            t.name
                    end name, 
                    t.name brand,
                    SUM(price) PRICE2_SUM, 
                    SUM(cost) cost_sum,
                    SUM(cnt) cnt,
                    (SUM(price) - SUM(cost)) disc_sum,
                    case when SUM(price) != 0 then
                    ROUND(SUM(cost)/SUM(price)*100,2)
                    else 0 end pr_disc,
                    case when sum(sum(cnt)) over() !=0 then
                    sum(cnt)/sum(sum(cnt)) over()*100 else 0 end pr_cnt,
                    case when sum(sum(price)) over() !=0 then
                    sum(price)/sum(sum(price)) over()*100 else 0 end pr,
                    round(case when SUM(decode(r.tp,1,price2,0))=0 then 0 else SUM(decode(r.tp,2,abs(price2),0))/SUM(decode(r.tp,1,price2,0))*100 end,2) proc_back_sum,
                    sum(price2_back_sum) price2_back_sum,   
                    case when SUM(decode(r.tp,1,cnt,0))=0 then 0 else SUM(decode(r.tp,1,abs(cnt),0)) end sale_cnt,
                    case when SUM(decode(r.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(r.tp,2,abs(cnt),0)) end back_cnt,  
                    sum(price2_sale_sum) price2_sale_sum
                    from (
                        SELECT szon,sum(price) price,sum(price2) price2,sum(price2val) price2val, tp,
                        sum(cost) cost,sum(cost_buh) cost_buh,sum(cnt) cnt,0 cntp,0 rst,
                        SUM(decode(m.tp,2,abs(price2),0)) price2_back_sum,
                        SUM(decode(m.tp,1,price2,0)) price2_sale_sum
                        FROM mv_sale1 m
                        WHERE dat BETWEEN v_d1_2 AND v_d2_2
                        and part = 1  and isinet=1 and inet_tp=1 and szon=c1.szon
                        and (v_without_pkg is null or brand != 66)
                        and trpoint in (select id from trpoint where report_tp = 1 ) 
                        group by szon,tp
                        having sum(cnt) !=0 
                    ) r
                    right JOIN col t ON  r.szon = t.id
                    GROUP BY t.id,t.name,(v_d1_2), (v_d2_2),t.name
                    having sum(cnt) !=0
                )loop      
                    
                    if nvl(v_add_period,0) = 1 then
                        return_row_p2.name := v_d1_2||'-'||v_d2_2;
                    else
                        return_row_p2.name := c1.name;
                    end if;
                    
                    return_row_p2.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                    return_row_p2.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                    return_row_p2.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                    return_row_p2.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                    return_row_p2.PR := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.PR_CNT := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                    return_row_p2.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                    return_row_p2.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                    
                    is_p2 := 1;
                end loop; 
                
                if is_p2 != 1 then                             
                    return_row_p2.name := v_d1_2||'-'||v_d2_2;
                    return_row_p2.PRICE2_SALE_SUM := 0;
                    return_row_p2.PRICE2_BACK_SUM := 0;
                    return_row_p2.PROC_BACK_SUM := 0;
                    return_row_p2.PRICE2_SUM := 0;
                    return_row_p2.DISC_SUM := 0;
                    return_row_p2.COST_SUM := 0;
                    return_row_p2.PR_DISC := 0;
                    return_row_p2.PR := 0;
                    return_row_p2.PR_CNT := 0;
                    return_row_p2.CNT := 0;
                    return_row_p2.SALE_CNT := 0;
                    return_row_p2.BACK_CNT := 0;
                end if;
                   
                return_row_diff.NAME :='<div width="100%" align="right"><b>Різниця</b></div>'; 
                return_row_diff.PRICE2_SALE_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PROC_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PROC_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PROC_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.DISC_SUM := '<b>'||to_char(round(to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.COST_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_DISC := '<b>'||to_char(round(to_number(return_row_p2.PR_DISC,'999G999G999G999G990D00') - to_number(return_row_p1.PR_DISC,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR :=  '<b>'||to_char(round(to_number(return_row_p2.PR,'999G999G999G999G990D00') - to_number(return_row_p1.PR,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_CNT :=  '<b>'||to_char(round(to_number(return_row_p2.PR_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.PR_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.CNT := '<b>'||to_char(round(to_number(return_row_p2.cnt,'999G999G999G999G990D00') - to_number(return_row_p1.cnt,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.SALE_CNT := '<b>'||to_char(round(to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.BACK_CNT := '<b>'||to_char(round(to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                    
                pipe row(return_row_p1);
                pipe row(return_row_p2);
                pipe row(return_row_diff);
            else
                pipe row(return_row_p1);
            end if;
            
            return_row_total_p1.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.PRICE2_SUM := to_char(to_number(return_row_total_p1.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.DISC_SUM := to_char(to_number(return_row_total_p1.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.COST_SUM := to_char(to_number(return_row_total_p1.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.CNT := to_char(to_number(return_row_total_p1.CNT,'999G999G999G999G990D00') + to_number(return_row_p1.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.SALE_CNT := to_char(to_number(return_row_total_p1.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p1.BACK_CNT := to_char(to_number(return_row_total_p1.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            
            if nvl(v_add_period,0) = 1 then
                return_row_total_p2.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.PRICE2_SUM := to_char(to_number(return_row_total_p2.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.DISC_SUM := to_char(to_number(return_row_total_p2.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.COST_SUM := to_char(to_number(return_row_total_p2.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.CNT := to_char(to_number(return_row_total_p2.CNT,'999G999G999G999G990D00') + to_number(return_row_p2.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.SALE_CNT := to_char(to_number(return_row_total_p2.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_total_p2.BACK_CNT := to_char(to_number(return_row_total_p2.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');    
            end if;
            
            return_row_p1 := null;
            return_row_p2 := null;
            return_row_diff := null;  
            
            is_p1 := 0;
            is_p2 := 0;
        end loop;
              
    return_row_total_p1.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
    return_row_total_p1.PRICE2_SALE_SUM := '<b>'||return_row_total_p1.PRICE2_SALE_SUM ||'</b>';
    return_row_total_p1.PRICE2_BACK_SUM := '<b>'||return_row_total_p1.PRICE2_BACK_SUM ||'</b>';
    return_row_total_p1.PRICE2_SUM := '<b>'||return_row_total_p1.PRICE2_SUM ||'</b>';
    return_row_total_p1.DISC_SUM := '<b>'||return_row_total_p1.DISC_SUM ||'</b>';
    return_row_total_p1.COST_SUM := '<b>'||return_row_total_p1.COST_SUM ||'</b>';
    return_row_total_p1.CNT := '<b>'||return_row_total_p1.CNT ||'</b>';
    return_row_total_p1.SALE_CNT := '<b>'||return_row_total_p1.SALE_CNT ||'</b>';
    return_row_total_p1.BACK_CNT := '<b>'||return_row_total_p1.BACK_CNT ||'</b>';
        
    if nvl(v_add_period,0) = 1 then
        return_row_total_p2.PROC_BACK_SUM := '<b>'||to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') * 100 / to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00') ||'</b>';
        return_row_total_p2.PRICE2_SALE_SUM := '<b>'||return_row_total_p2.PRICE2_SALE_SUM ||'</b>';
        return_row_total_p2.PRICE2_BACK_SUM := '<b>'||return_row_total_p2.PRICE2_BACK_SUM ||'</b>';
        return_row_total_p2.PRICE2_SUM := '<b>'||return_row_total_p2.PRICE2_SUM ||'</b>';
        return_row_total_p2.DISC_SUM := '<b>'||return_row_total_p2.DISC_SUM ||'</b>';
        return_row_total_p2.COST_SUM := '<b>'||return_row_total_p2.COST_SUM ||'</b>';
        return_row_total_p2.CNT := '<b>'||return_row_total_p2.CNT ||'</b>';
        return_row_total_p2.SALE_CNT := '<b>'||return_row_total_p2.SALE_CNT ||'</b>';
        return_row_total_p2.BACK_CNT := '<b>'||return_row_total_p2.BACK_CNT ||'</b>';
    end if;   
    
    pipe row(return_row_total_p1);
    if nvl(v_add_period,0) = 1 then   
        pipe row(return_row_total_p2);
    end if;
end sales_by_szon;
-------------------------

-- отримуєм розкладені періоди по дням
function get_date_table(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date
) RETURN date_t pipelined as
    v_cnt number;
    return_row date_r;
    v_d1 date;
    v_d2 date;
begin
    if d1_2 is not null then
        select max(cnt) into v_cnt from(
            select d2_1 - d1_1 cnt from dual
            union all
            select d2_2 - d1_2 cnt from dual
        );
    else
        select max(cnt) into v_cnt from(
            select d2_1 - d1_1 cnt from dual
        );    
    end if;
    
    for n in 0..v_cnt loop
        if d1_2 is not null then
            if d1_1 < d1_2 then
                v_d1 := d1_1 + n;
                v_d2 := d1_2 + n;
                
                if v_d1 <= d2_1 then
                    return_row.p1 := v_d1;
                else
                    return_row.p1 := null;
                end if;
                
                if v_d2 <= d2_2 then
                    return_row.p2 := v_d2;
                else
                    return_row.p2 := null;
                end if;
                pipe row(return_row);
            else
                v_d2 := d1_1 + n;
                v_d1 := d1_2 + n;
                
                if v_d2 <= d2_1 then
                    return_row.p1 := v_d1;
                else
                    return_row.p1 := null;
                end if;
                
                if v_d1 <= d2_2 then
                    return_row.p2 := v_d2;
                else
                    return_row.p2 := null;
                end if;
                pipe row(return_row);
            end if;
        else
            v_d1 := d1_1 + n;
            return_row.p1 := v_d1;
            return_row.p2 := null;
            pipe row(return_row);
        end if;
    end loop;
end get_date_table;
-------------------

-- продажі -> по дням з урахуванням повернень
function sales_by_day_with_back(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_day_with_back_t pipelined as
    return_row1 sales_by_day_with_back_r;
    return_row2 sales_by_day_with_back_r;
    
    V_SALE_SUM_P1 number := 0;
    V_BACK_SUM_P1 number  := 0;
    V_PRICE2_SUM_P1 number  := 0;
    V_CNT_P1 number  := 0;
    
    V_SALE_SUM_P2 number  := 0;
    V_BACK_SUM_P2 number  := 0;
    V_PRICE2_SUM_P2 number  := 0;
    V_CNT_P2 number  := 0;
    
    vd1_1 date;
    vd2_1 date;
    vd1_2 date;
    vd2_2 date;
    
begin
    if nvl(v_add_period,0)=1 then
        if d1_1 < d1_2 then
            vd1_1 := d1_1;
            vd2_1 := d2_1;
            vd1_2 := d1_2;
            vd2_2 := d2_2;
        else
            vd1_1 := d1_2;
            vd2_1 := d2_2;
            vd1_2 := d1_1;
            vd2_2 := d2_1;
        end if;
    else
        vd1_1 := d1_1;
        vd2_1 := d2_1;
    end if;
    for c1 in (
        select * from table(
            analytic_pkg.get_date_table(
                d1_1 => d1_1,
                d2_1 => d2_1,
                d1_2 => d1_2,
                d2_2 => d2_2
            )   
        )
    ) loop

        select * INTO
            return_row1.DAT, return_row1.PRICE2_SUM, return_row1.CNT, return_row1.PR, return_row1.PR_ALL, return_row1.SALE_SUM, return_row1.BACK_SUM,  return_row1.PR_DISC 
        from(SELECT 
            dat, to_char(SUM(price2),'999G999G999G999G990D00') price2_sum, SUM(cnt) cnt,
            case 
                when SUM(SUM(price)) over() != 0 then ROUND(SUM(price)/SUM(SUM(price)) over()*100,2) 
                else 0 
            end pr,
            case 
                when sum(sum(cnt)) over() !=0 then round(sum(cnt)/sum(sum(cnt)) over()*100,2) 
                else 0 
            end pr_all,
            to_char(sum(sale_sum),'999G999G999G999G990D00') sale_sum, to_char(sum(back_sum),'999G999G999G999G990D00') back_sum,
            case 
                when SUM(price) != 0 then ROUND(SUM(cost)/SUM(price)*100,2)
                else 0 
            end pr_disc
        FROM(
            SELECT 
                trunc(dat) dat, szon, price, sum(price2) price2, price2val, cost, cost_buh, cnt,
                sum(case when tp=1 then price2 else 0 end ) sale_sum,
                sum(case when tp=2 then price2*-1 else 0 end ) back_sum
            FROM mv_sale1
            WHERE dat BETWEEN vd1_1 AND vd2_1
                and trpoint in (select id from trpoint where report_tp = 1 ) 
                and (v_without_pkg is null or brand != 66) and isinet=1 and inet_tp=1
            group by trunc(dat), szon, price, price2, price2val, cost, cost_buh, cnt
        ) r
        GROUP BY dat
        having sum(cnt) !=0)
        where dat = c1.p1;
        
        if return_row1.SALE_SUM is not null then
            V_SALE_SUM_P1 := V_SALE_SUM_P1 + nvl(to_number(return_row1.SALE_SUM,'999G999G999G999G990D00'), 0);
            V_BACK_SUM_P1 := V_BACK_SUM_P1 + nvl(to_number(return_row1.BACK_SUM,'999G999G999G999G990D00'), 0);
            V_PRICE2_SUM_P1 := V_PRICE2_SUM_P1 + nvl(to_number(return_row1.PRICE2_SUM,'999G999G999G999G990D00'), 0);
            V_CNT_P1 := V_CNT_P1 + nvl(to_number(return_row1.CNT,'999G999G999G999G990D00'), 0);
        end if;
        
        if nvl(v_add_period,0)=1 then
            select * INTO
                return_row2.DAT, return_row2.PRICE2_SUM, return_row2.CNT, return_row2.PR, return_row2.PR_ALL, return_row2.SALE_SUM, return_row2.BACK_SUM,  return_row2.PR_DISC  
            from(
            SELECT 
                dat, to_char(SUM(price2),'999G999G999G999G990D00') price2_sum, SUM(cnt) cnt,
                case 
                    when SUM(SUM(price)) over() != 0 then ROUND(SUM(price)/SUM(SUM(price)) over()*100,2) 
                    else 0 
                end pr,
                case 
                    when sum(sum(cnt)) over() !=0 then round(sum(cnt)/sum(sum(cnt)) over()*100,2) 
                    else 0 
                end pr_all,
                to_char(sum(sale_sum),'999G999G999G999G990D00') sale_sum, to_char(sum(back_sum),'999G999G999G999G990D00') back_sum,
                case 
                    when SUM(price) != 0 then ROUND(SUM(cost)/SUM(price)*100,2)
                    else 0 
                end pr_disc
            FROM(
                SELECT 
                    trunc(dat) dat, szon, price, sum(price2) price2, price2val, cost, cost_buh, cnt,
                    sum(case when tp=1 then price2 else 0 end ) sale_sum,
                    sum(case when tp=2 then price2*-1 else 0 end ) back_sum
                FROM mv_sale1
                WHERE dat BETWEEN vd1_2 AND vd2_2
                    and trpoint in (select id from trpoint where report_tp = 1 ) 
                    and (v_without_pkg is null or brand != 66) and isinet=1 and inet_tp=1
                group by trunc(dat), szon, price, price2, price2val, cost, cost_buh, cnt
            ) r
            
            GROUP BY dat
            having sum(cnt) !=0)
            where dat = c1.p2;
            
             if return_row2.SALE_SUM is not null then
                V_SALE_SUM_P2 := V_SALE_SUM_P2 + nvl(to_number(return_row2.SALE_SUM,'999G999G999G999G990D00'), 0);
                V_BACK_SUM_P2 := V_BACK_SUM_P2 + nvl(to_number(return_row2.BACK_SUM,'999G999G999G999G990D00'), 0);
                V_PRICE2_SUM_P2 := V_PRICE2_SUM_P2 + nvl(to_number(return_row2.PRICE2_SUM,'999G999G999G999G990D00'), 0);
                V_CNT_P2 := V_CNT_P2 + nvl(to_number(return_row2.CNT,'999G999G999G999G990D00'), 0);
            end if;
        end if;
        
        if nvl(v_add_period,0)=1 then
            return_row1.DAT := return_row1.DAT ||' / <b>'|| return_row2.DAT||'</b>';
            return_row1.SALE_SUM := return_row1.SALE_SUM ||' / <b>'|| return_row2.SALE_SUM||'</b>';
            return_row1.BACK_SUM := return_row1.BACK_SUM ||' / <b>'|| return_row2.BACK_SUM||'</b>';
            return_row1.PRICE2_SUM := return_row1.PRICE2_SUM ||' / <b>'|| return_row2.PRICE2_SUM||'</b>';
            return_row1.PR_DISC := return_row1.PR_DISC ||' / <b>'|| return_row2.PR_DISC||'</b>';
            return_row1.PR := return_row1.PR ||' / <b>'|| return_row2.PR||'</b>';
            return_row1.PR_ALL := return_row1.PR_ALL ||' / <b>'|| return_row2.PR_ALL||'</b>';
            return_row1.CNT := return_row1.CNT ||' / <b>'|| return_row2.CNT||'</b>';
        end if;
        
        pipe row (return_row1);
        
        return_row1 := null;
        return_row2 := null;
    end loop;
    
    return_row1.DAT := '<b>Всього</b>';
    if nvl(v_add_period,0)=1 then
        return_row1.SALE_SUM := '<b>'||to_char(V_SALE_SUM_P1,'999G999G999G999G990D00') ||' / '|| to_char(V_SALE_SUM_P2,'999G999G999G999G990D00')||'</b>';
        return_row1.BACK_SUM := '<b>'||to_char(V_BACK_SUM_P1,'999G999G999G999G990D00') ||' / '|| to_char(V_BACK_SUM_P2,'999G999G999G999G990D00')||'</b>';
        return_row1.PRICE2_SUM := '<b>'||to_char(V_PRICE2_SUM_P1,'999G999G999G999G990D00') ||' / '|| to_char(V_PRICE2_SUM_P2,'999G999G999G999G990D00')||'</b>';
        return_row1.CNT := '<b>'||V_CNT_P1 ||' / '|| V_CNT_P2||'</b>';
    else
        return_row1.SALE_SUM := '<b>'||to_char(V_SALE_SUM_P1,'999G999G999G999G990D00') || '</b>';
        return_row1.BACK_SUM := '<b>'||to_char(V_BACK_SUM_P1,'999G999G999G999G990D00') ||'</b>';
        return_row1.PRICE2_SUM := '<b>'||to_char(V_PRICE2_SUM_P1,'999G999G999G999G990D00') || '</b>';
        return_row1.CNT := '<b>'||V_CNT_P1 ||'</b>';
    end if;
    pipe row (return_row1);
end sales_by_day_with_back;
-------------------

-- продажі -> по даті продажу
function sales_by_day(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_day_t pipelined as
    return_row1 sales_by_day_r;
    return_row2 sales_by_day_r;
    
    V_SALE_SUM_P1 number := 0;
    V_BACK_SUM_P1 number  := 0;
    V_PRICE2_SUM_P1 number  := 0;
    V_CNT_P1 number  := 0;
    
    V_SALE_SUM_P2 number  := 0;
    V_BACK_SUM_P2 number  := 0;
    V_PRICE2_SUM_P2 number  := 0;
    V_CNT_P2 number  := 0;
    
    vd1_1 date;
    vd2_1 date;
    vd1_2 date;
    vd2_2 date;
begin
    if nvl(v_add_period,0)=1 then
        if d1_1 < d1_2 then
            vd1_1 := d1_1;
            vd2_1 := d2_1;
            vd1_2 := d1_2;
            vd2_2 := d2_2;
        else
            vd1_1 := d1_2;
            vd2_1 := d2_2;
            vd1_2 := d1_1;
            vd2_2 := d2_1;
        end if;
    else
        vd1_1 := d1_1;
        vd2_1 := d2_1;
    end if;
    
    for c1 in (
        select * from table(
            analytic_pkg.get_date_table(
                d1_1 => d1_1,
                d2_1 => d2_1,
                d1_2 => d1_2,
                d2_2 => d2_2
            )   
        )
    ) loop
        select 
            dat, cnt, to_char(price2_sum,'999G999G999G999G990D00'), to_char(back_sum,'999G999G999G999G990D00'), to_char(sale_sum,'999G999G999G999G990D00')
        into
            return_row1.DAT, return_row1.CNT, return_row1.PRICE2_SUM, return_row1.BACK_SUM, return_row1.SALE_SUM
        from(
            select 
            t.chdat dat,
            sum(t.cnt*t.dc) cnt,
            sum(t.total_sum*t.dc) price2_sum,
            sum(case when t.tp=1 then t.total_sum else 0 end) sale_sum,
            sum(case when t.tp=2 then t.total_sum else 0 end) back_sum
            from table(inet_sale.get_sale_back_date_tbl(v_sdat=>vd1_1,v_edat=>vd2_1 ,v_inet_tp=>1)) t
            group by t.chdat
        )where dat = c1.p1;
        
        if return_row1.SALE_SUM is not null then
            V_SALE_SUM_P1 := V_SALE_SUM_P1 + nvl(to_number(return_row1.SALE_SUM,'999G999G999G999G990D00'), 0);
            V_BACK_SUM_P1 := V_BACK_SUM_P1 + nvl(to_number(return_row1.BACK_SUM,'999G999G999G999G990D00'), 0);
            V_PRICE2_SUM_P1 := V_PRICE2_SUM_P1 + nvl(to_number(return_row1.PRICE2_SUM,'999G999G999G999G990D00'), 0);
            V_CNT_P1 := V_CNT_P1 + nvl(to_number(return_row1.CNT,'999G999G999G999G990D00'), 0);
        end if;
        
        if nvl(v_add_period,0)=1 then
            select 
                dat, cnt, to_char(price2_sum,'999G999G999G999G990D00'), to_char(back_sum,'999G999G999G999G990D00'), to_char(sale_sum,'999G999G999G999G990D00')
            into
                return_row2.DAT, return_row2.CNT, return_row2.PRICE2_SUM, return_row2.BACK_SUM, return_row2.SALE_SUM
            from(
                select 
                t.chdat dat,
                sum(t.cnt*t.dc) cnt,
                sum(t.total_sum*t.dc) price2_sum,
                sum(case when t.tp=1 then t.total_sum else 0 end) sale_sum,
                sum(case when t.tp=2 then t.total_sum else 0 end) back_sum
                from table(inet_sale.get_sale_back_date_tbl(v_sdat=>vd1_2,v_edat=>vd2_2 ,v_inet_tp=>1)) t
                group by t.chdat
            )where dat = c1.p2;
            
             if return_row2.SALE_SUM is not null then
                V_SALE_SUM_P2 := V_SALE_SUM_P2 + nvl(to_number(return_row2.SALE_SUM,'999G999G999G999G990D00'), 0);
                V_BACK_SUM_P2 := V_BACK_SUM_P2 + nvl(to_number(return_row2.BACK_SUM,'999G999G999G999G990D00'), 0);
                V_PRICE2_SUM_P2 := V_PRICE2_SUM_P2 + nvl(to_number(return_row2.PRICE2_SUM,'999G999G999G999G990D00'), 0);
                V_CNT_P2 := V_CNT_P2 + nvl(to_number(return_row2.CNT,'999G999G999G999G990D00'), 0);
            end if;
        
            return_row1.DAT := return_row1.DAT ||' / <b>'|| return_row2.DAT||'</b>';
            return_row1.SALE_SUM := return_row1.SALE_SUM ||' / <b>'|| return_row2.SALE_SUM||'</b>';
            return_row1.BACK_SUM := return_row1.BACK_SUM ||' / <b>'|| return_row2.BACK_SUM||'</b>';
            return_row1.PRICE2_SUM := return_row1.PRICE2_SUM ||' / <b>'|| return_row2.PRICE2_SUM||'</b>';
            return_row1.CNT := return_row1.CNT ||' / <b>'|| return_row2.CNT||'</b>';
        end if;
        
        pipe row (return_row1);
        
        return_row1 := null;
        return_row2 := null;
    end loop;
    
    return_row1.DAT := '<b>Всього</b>';
    if nvl(v_add_period,0)=1 then
        return_row1.SALE_SUM := '<b>'||to_char(V_SALE_SUM_P1,'999G999G999G999G990D00') ||' / '|| to_char(V_SALE_SUM_P2,'999G999G999G999G990D00')||'</b>';
        return_row1.BACK_SUM := '<b>'||to_char(V_BACK_SUM_P1,'999G999G999G999G990D00') ||' / '|| to_char(V_BACK_SUM_P2,'999G999G999G999G990D00')||'</b>';
        return_row1.PRICE2_SUM := '<b>'||to_char(V_PRICE2_SUM_P1,'999G999G999G999G990D00') ||' / '|| to_char(V_PRICE2_SUM_P2,'999G999G999G999G990D00')||'</b>';
        return_row1.CNT := '<b>'||V_CNT_P1 ||' / '|| V_CNT_P2||'</b>';
    else
        return_row1.SALE_SUM := '<b>'||to_char(V_SALE_SUM_P1,'999G999G999G999G990D00') ||'</b>';
        return_row1.BACK_SUM := '<b>'||to_char(V_BACK_SUM_P1,'999G999G999G999G990D00') ||'</b>';
        return_row1.PRICE2_SUM := '<b>'||to_char(V_PRICE2_SUM_P1,'999G999G999G999G990D00') ||'</b>';
        return_row1.CNT := '<b>'||V_CNT_P1 ||'</b>';
    end if;
    
    pipe row (return_row1);
end sales_by_day;
-------------------

-- продажі -> по магазинам
function sales_by_shops_day(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_by_shops_t_char pipelined
as
    v_d1_1 date;
    v_d2_1 date;
    v_d1_2 date;
    v_d2_2 date;
    
    return_row_p1 sales_by_shops_r_char;
    return_row_p2 sales_by_shops_r_char;
    return_row_diff sales_by_shops_r_char;
    return_row_jur_p1 sales_by_shops_r_char;
    return_row_jur_p2 sales_by_shops_r_char;
    return_row_total_p1 sales_by_shops_r_char;
    return_row_total_p2 sales_by_shops_r_char;
    
    is_p1 number := 0;
    is_p2 number := 0;
    l_url varchar2(4000);
begin
    if d1_1 > d1_2 then
        v_d1_1 := d1_2;
        v_d2_1 := d2_2;
        
        v_d1_2 := d1_1;
        v_d2_2 := d2_1;
    else
        v_d1_2 := d1_2;
        v_d2_2 := d2_2;
        
        v_d1_1 := d1_1;
        v_d2_1 := d2_1;
    end if;
    
    return_row_total_p1.name := '<div width="100%" ><b>Всього'||' '|| v_d1_1 || '-' || v_d2_1||'</b></div>';
    return_row_total_p1.PRICE2_SALE_SUM := 0;
    return_row_total_p1.PRICE2_BACK_SUM := 0;
    return_row_total_p1.PRICE2_SUM := 0;
    return_row_total_p1.DISC_SUM := 0;
    return_row_total_p1.COST_SUM := 0;
    return_row_total_p1.CNT := 0;
    return_row_total_p1.SALE_CNT := 0;
    return_row_total_p1.BACK_CNT := 0;
        
    return_row_total_p2.name := '<div width="100%" ><b>Всього'||' '|| v_d1_2 || '-' || v_d2_2||'</b></div>';
    return_row_total_p2.PRICE2_SALE_SUM := 0;
    return_row_total_p2.PRICE2_BACK_SUM := 0;
    return_row_total_p2.PRICE2_SUM := 0;
    return_row_total_p2.DISC_SUM := 0;
    return_row_total_p2.COST_SUM := 0;
    return_row_total_p2.CNT := 0;
    return_row_total_p2.SALE_CNT := 0;
    return_row_total_p2.BACK_CNT := 0;
    
    for c1 in (
        select distinct r.jur, j.name from(
            select distinct r.trpoint, t.name, t.jur from(
                select distinct trpoint from mv_sale1
                where dat between d1_1 and d2_1 
                    union all
                select distinct trpoint from mv_sale1
                where dat between d1_2 and d2_2
            ) r
            left join trpoint t on t.id=r.trpoint 
            where t.report_tp = 1
        ) r
        left join jur j on j.id=r.jur
    )loop
        -- виводим заголовок по юр. лицю
        return_row_p1:= null;
        return_row_p1.name := '<b>'||c1.name||'</b>';
        pipe row(return_row_p1);
        return_row_p1:= null;
        
        return_row_jur_p1.name := '<div width="100%" align="right"><b>Сума по юр. лицю'||' '|| v_d1_1 || '-' || v_d2_1||'</b></div>';
        return_row_jur_p1.PRICE2_SALE_SUM := 0;
        return_row_jur_p1.PRICE2_BACK_SUM := 0;
        return_row_jur_p1.PRICE2_SUM := 0;
        return_row_jur_p1.DISC_SUM := 0;
        return_row_jur_p1.COST_SUM := 0;
        return_row_jur_p1.CNT := 0;
        return_row_jur_p1.SALE_CNT := 0;
        return_row_jur_p1.BACK_CNT := 0;
        
        return_row_jur_p2.name := '<div width="100%" align="right"><b>Сума по юр. лицю'||' '|| v_d1_2 || '-' || v_d2_2||'</b></div>';
        return_row_jur_p2.PRICE2_SALE_SUM := 0;
        return_row_jur_p2.PRICE2_BACK_SUM := 0;
        return_row_jur_p2.PRICE2_SUM := 0;
        return_row_jur_p2.DISC_SUM := 0;
        return_row_jur_p2.COST_SUM := 0;
        return_row_jur_p2.CNT := 0;
        return_row_jur_p2.SALE_CNT := 0;
        return_row_jur_p2.BACK_CNT := 0;
        
        for c2 in (
            select distinct r.trpoint, t.name from(
                select distinct trpoint from mv_sale1
                where dat between d1_1 and d2_1 and sjur=c1.jur and part = 1 and isinet=1 and inet_tp=1
                    union all
                select distinct trpoint from mv_sale1
                where dat between d1_2 and d2_2 and sjur=c1.jur and part = 1 and isinet=1 and inet_tp=1
            ) r
            left join trpoint t on t.id=r.trpoint
            where t.report_tp = 1
        )loop  
            for c3 in (          
                SELECT 
                t.id,
                (v_d1_1) dat1,
                (v_d2_1) dat2,
                t.trpoint_name tt_name,
                case
                    when nvl(v_add_period,0) = 1 then
                        t.trpoint_name ||' '|| v_d1_1 || '-' || v_d2_1
                    else
                        t.trpoint_name
                end name,
                t.jur,
                t.sale_sum price2_sale_sum,
                t.back_sum price2_back_sum,
                t.total_sum price2_sum,
                t.cost_sum,
                t.sum_cnt cnt,
                t.total_sum-t.cost_sum disc_sum,
                t.proc_back_sum,
                t.sale_cnt,
                t.back_cnt,
                pr,
                pr_disc,
                pr_cnt
                FROM
                (
                    select 
                        tr.id,
                        j.name jur,
                        t.trpoint_name,
                        sum(t.cnt*t.dc) sum_cnt,
                        sum(t.total_sum*t.dc) total_sum,
                        sum(t.cost*t.dc) cost_sum,
                        sum(case when t.tp=1 then t.total_sum else 0 end ) sale_sum,
                        sum(case when t.tp=2 then t.total_sum else 0 end ) back_sum,
                        case when SUM(t.total_sum) != 0 then
                        ROUND(SUM(cost)/SUM(t.total_sum)*100,2)
                        else 0 end pr_disc,
                        case when sum(sum(t.cnt)) over() !=0 then
                        sum(t.cnt)/sum(sum(t.cnt)) over()*100 else 0 end pr_cnt,
                        case when sum(sum(t.total_sum)) over() !=0 then
                        sum(t.total_sum)/sum(sum(t.total_sum)) over()*100 else 0 end pr,
                        round(case when SUM(decode(t.tp,1,t.total_sum,0))=0 then 0 else SUM(decode(t.tp,2,abs(t.total_sum),0))/SUM(decode(t.tp,1,t.total_sum,0))*100 end,2) proc_back_sum,
                        case when SUM(decode(t.tp,1,t.cnt,0))=0 then 0 else SUM(decode(t.tp,1,abs(t.cnt),0)) end sale_cnt,
                        case when SUM(decode(t.tp,2,abs(t.cnt),0))=0 then 0 else SUM(decode(t.tp,2,abs(t.cnt),0)) end back_cnt
                    from table(inet_sale.get_sale_back_tbl(v_sdat=>v_d1_1,v_edat=>v_d2_1 ,v_inet_tp=>1)) t
                    join jur j on t.jur=j.id
                    left join trpoint tr on t.trpoint_name = tr.name
                    where tr.id=c2.trpoint
                    group by j.name, tr.id, t.trpoint_name
                ) t   
            )loop
                if nvl(v_add_period,0) = 1 then
                    return_row_p1.name := c2.name||' '||v_d1_1||'-'||v_d2_1;
                else
                    return_row_p1.name := c2.name;
                end if;
                
                return_row_p1.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                return_row_p1.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                return_row_p1.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                return_row_p1.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                return_row_p1.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                return_row_p1.PR := to_char(c3.PR,'999G999G999G999G990D00');
                return_row_p1.PR_CNT := to_char(c3.PR_CNT,'999G999G999G999G990D00');
                return_row_p1.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                return_row_p1.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                return_row_p1.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                
                is_p1 := 1;
            end loop;
            
            if is_p1 != 1 then                                 
                return_row_p1.name := c2.name||' '||v_d1_1||'-'||v_d2_1;
                return_row_p1.PRICE2_SALE_SUM := 0;
                return_row_p1.PRICE2_BACK_SUM := 0;
                return_row_p1.PROC_BACK_SUM := 0;
                return_row_p1.PRICE2_SUM := 0;
                return_row_p1.DISC_SUM := 0;
                return_row_p1.COST_SUM := 0;
                return_row_p1.PR_DISC := 0;
                return_row_p1.PR := 0;
                return_row_p1.PR_CNT := 0;
                return_row_p1.CNT := 0;
                return_row_p1.SALE_CNT := 0;
                return_row_p1.BACK_CNT := 0;
            end if;
            
            return_row_jur_p1.PRICE2_SALE_SUM := to_char(to_number(return_row_jur_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.PRICE2_BACK_SUM := to_char(to_number(return_row_jur_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.PRICE2_SUM := to_char(to_number(return_row_jur_p1.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.DISC_SUM := to_char(to_number(return_row_jur_p1.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.COST_SUM := to_char(to_number(return_row_jur_p1.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.CNT := to_char(to_number(return_row_jur_p1.CNT,'999G999G999G999G990D00') + to_number(return_row_p1.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.SALE_CNT := to_char(to_number(return_row_jur_p1.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_jur_p1.BACK_CNT := to_char(to_number(return_row_jur_p1.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            
            if nvl(v_add_period,0) = 1 then
                for c3 in (     
                    -- період 2 --
                    SELECT 
                t.id,
                (v_d1_2) dat1,
                (v_d2_2) dat2,
                t.trpoint_name tt_name,
                case
                    when nvl(v_add_period,0) = 1 then
                        t.trpoint_name ||' '|| v_d1_2 || '-' || v_d2_2
                    else
                        t.trpoint_name
                end name,
                t.jur,
                t.sale_sum price2_sale_sum,
                t.back_sum price2_back_sum,
                t.total_sum price2_sum,
                t.cost_sum,
                t.sum_cnt cnt,
                t.total_sum-t.cost_sum disc_sum,
                t.proc_back_sum,
                t.sale_cnt,
                t.back_cnt,
                pr,
                pr_disc,
                pr_cnt
                FROM
                (
                    select 
                        tr.id,
                        j.name jur,
                        t.trpoint_name,
                        sum(t.cnt*t.dc) sum_cnt,
                        sum(t.total_sum*t.dc) total_sum,
                        sum(t.cost*t.dc) cost_sum,
                        sum(case when t.tp=1 then t.total_sum else 0 end ) sale_sum,
                        sum(case when t.tp=2 then t.total_sum else 0 end ) back_sum,
                        case when SUM(t.total_sum) != 0 then
                        ROUND(SUM(cost)/SUM(t.total_sum)*100,2)
                        else 0 end pr_disc,
                        case when sum(sum(t.cnt)) over() !=0 then
                        sum(t.cnt)/sum(sum(t.cnt)) over()*100 else 0 end pr_cnt,
                        case when sum(sum(t.total_sum)) over() !=0 then
                        sum(t.total_sum)/sum(sum(t.total_sum)) over()*100 else 0 end pr,
                        round(case when SUM(decode(t.tp,1,total_sum,0))=0 then 0 else SUM(decode(t.tp,2,abs(total_sum),0))/SUM(decode(t.tp,1,total_sum,0))*100 end,2) proc_back_sum,
                        case when SUM(decode(t.tp,1,cnt,0))=0 then 0 else SUM(decode(t.tp,1,abs(cnt),0)) end sale_cnt,
                        case when SUM(decode(t.tp,2,abs(cnt),0))=0 then 0 else SUM(decode(t.tp,2,abs(cnt),0)) end back_cnt
                    from table(inet_sale.get_sale_back_tbl(v_sdat=>v_d1_2,v_edat=>v_d2_2 ,v_inet_tp=>1)) t
                    join jur j on t.jur=j.id
                    left join trpoint tr on t.trpoint_name = tr.name
                    where tr.id=c2.trpoint
                    group by j.name, tr.id, t.trpoint_name
                ) t 
                )loop      
                    
                    if nvl(v_add_period,0) = 1 then
                        return_row_p2.name := c2.name||' '||v_d1_2||'-'||v_d2_2;
                    else
                        return_row_p2.name := c2.name;
                    end if;
                    
                    return_row_p2.PRICE2_SALE_SUM := to_char(c3.PRICE2_SALE_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_BACK_SUM := to_char(c3.PRICE2_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PROC_BACK_SUM := to_char(c3.PROC_BACK_SUM,'999G999G999G999G990D00');
                    return_row_p2.PRICE2_SUM := to_char(c3.PRICE2_SUM,'999G999G999G999G990D00');
                    return_row_p2.DISC_SUM := to_char(c3.DISC_SUM,'999G999G999G999G990D00');
                    return_row_p2.COST_SUM := to_char(c3.COST_SUM,'999G999G999G999G990D00');
                    return_row_p2.PR_DISC := to_char(c3.PR_DISC,'999G999G999G999G990D00');
                    return_row_p2.PR := to_char(c3.PR,'999G999G999G999G990D00');
                    return_row_p2.PR_CNT := to_char(c3.PR_CNT,'999G999G999G999G990D00');
                    return_row_p2.CNT := to_char(c3.CNT,'999G999G999G999G990D00');
                    return_row_p2.SALE_CNT := to_char(c3.SALE_CNT,'999G999G999G999G990D00');
                    return_row_p2.BACK_CNT := to_char(c3.BACK_CNT,'999G999G999G999G990D00');
                    
                    is_p2 := 1;
                end loop; 
                
                if is_p2 != 1 then                                    
                    return_row_p2.name := c2.name||' '||v_d1_2||'-'||v_d2_2;
                    return_row_p2.PRICE2_SALE_SUM := 0;
                    return_row_p2.PRICE2_BACK_SUM := 0;
                    return_row_p2.PROC_BACK_SUM := 0;
                    return_row_p2.PRICE2_SUM := 0;
                    return_row_p2.DISC_SUM := 0;
                    return_row_p2.COST_SUM := 0;
                    return_row_p2.PR_DISC := 0;
                    return_row_p2.PR := 0;
                    return_row_p2.PR_CNT := 0;
                    return_row_p2.CNT := 0;
                    return_row_p2.SALE_CNT := 0;
                    return_row_p2.BACK_CNT := 0;
                end if;
                
                return_row_jur_p2.PRICE2_SALE_SUM := to_char(to_number(return_row_jur_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.PRICE2_BACK_SUM := to_char(to_number(return_row_jur_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.PRICE2_SUM := to_char(to_number(return_row_jur_p2.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.DISC_SUM := to_char(to_number(return_row_jur_p2.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.COST_SUM := to_char(to_number(return_row_jur_p2.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.CNT := to_char(to_number(return_row_jur_p2.CNT,'999G999G999G999G990D00') + to_number(return_row_p2.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.SALE_CNT := to_char(to_number(return_row_jur_p2.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                return_row_jur_p2.BACK_CNT := to_char(to_number(return_row_jur_p2.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
                
     
                return_row_diff.NAME :='<div width="100%" align="right"><b>Різниця</b></div>'; 
                return_row_diff.PRICE2_SALE_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PROC_BACK_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PROC_BACK_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PROC_BACK_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PRICE2_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.PRICE2_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.PRICE2_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.DISC_SUM := '<b>'||to_char(round(to_number(return_row_p2.DISC_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.DISC_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.COST_SUM :=  '<b>'||to_char(round(to_number(return_row_p2.COST_SUM,'999G999G999G999G990D00') - to_number(return_row_p1.COST_SUM,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_DISC := '<b>'||to_char(round(to_number(return_row_p2.PR_DISC,'999G999G999G999G990D00') - to_number(return_row_p1.PR_DISC,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR :=  '<b>'||to_char(round(to_number(return_row_p2.PR,'999G999G999G999G990D00') - to_number(return_row_p1.PR,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.PR_CNT :=  '<b>'||to_char(round(to_number(return_row_p2.PR_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.PR_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.CNT := '<b>'||to_char(round(to_number(return_row_p2.cnt,'999G999G999G999G990D00') - to_number(return_row_p1.cnt,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.SALE_CNT := '<b>'||to_char(round(to_number(return_row_p2.SALE_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.SALE_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                return_row_diff.BACK_CNT := '<b>'||to_char(round(to_number(return_row_p2.BACK_CNT,'999G999G999G999G990D00') - to_number(return_row_p1.BACK_CNT,'999G999G999G999G990D00'),2),'999G999G999G999G990D00')||'</b>';
                    
                pipe row(return_row_p1);
                pipe row(return_row_p2);
                pipe row(return_row_diff);
            else
                pipe row(return_row_p1);
            end if;
            
            return_row_p1 := null;
            return_row_p2 := null;
            return_row_diff := null;  
            
            is_p1 := 0;
            is_p2 := 0;
        end loop;
              
        return_row_total_p1.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.PRICE2_SUM := to_char(to_number(return_row_total_p1.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.DISC_SUM := to_char(to_number(return_row_total_p1.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.COST_SUM := to_char(to_number(return_row_total_p1.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p1.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.CNT := to_char(to_number(return_row_total_p1.CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p1.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.SALE_CNT := to_char(to_number(return_row_total_p1.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p1.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        return_row_total_p1.BACK_CNT := to_char(to_number(return_row_total_p1.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p1.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
        
        if nvl(v_add_period,0) = 1 then
            return_row_total_p2.PRICE2_SALE_SUM := to_char(to_number(return_row_total_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.PRICE2_SALE_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.PRICE2_BACK_SUM := to_char(to_number(return_row_total_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.PRICE2_BACK_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.PRICE2_SUM := to_char(to_number(return_row_total_p2.PRICE2_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.PRICE2_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.DISC_SUM := to_char(to_number(return_row_total_p2.DISC_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.DISC_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.COST_SUM := to_char(to_number(return_row_total_p2.COST_SUM,'999G999G999G999G990D00') + to_number(return_row_jur_p2.COST_SUM,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.CNT := to_char(to_number(return_row_total_p2.CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p2.CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.SALE_CNT := to_char(to_number(return_row_total_p2.SALE_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p2.SALE_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');
            return_row_total_p2.BACK_CNT := to_char(to_number(return_row_total_p2.BACK_CNT,'999G999G999G999G990D00') + to_number(return_row_jur_p2.BACK_CNT,'999G999G999G999G990D00'),'999G999G999G999G990D00');    
        end if;
        
        return_row_jur_p1.PRICE2_SALE_SUM := '<b>'||return_row_jur_p1.PRICE2_SALE_SUM ||'</b>';
        return_row_jur_p1.PRICE2_BACK_SUM := '<b>'||return_row_jur_p1.PRICE2_BACK_SUM ||'</b>';
        return_row_jur_p1.PRICE2_SUM := '<b>'||return_row_jur_p1.PRICE2_SUM ||'</b>';
        return_row_jur_p1.DISC_SUM := '<b>'||return_row_jur_p1.DISC_SUM ||'</b>';
        return_row_jur_p1.COST_SUM := '<b>'||return_row_jur_p1.COST_SUM ||'</b>';
        return_row_jur_p1.CNT := '<b>'||return_row_jur_p1.CNT ||'</b>';
        return_row_jur_p1.SALE_CNT := '<b>'||return_row_jur_p1.SALE_CNT ||'</b>';
        return_row_jur_p1.BACK_CNT := '<b>'||return_row_jur_p1.BACK_CNT ||'</b>';
        
        if nvl(v_add_period,0) = 1 then
            return_row_jur_p2.PRICE2_SALE_SUM := '<b>'||return_row_jur_p2.PRICE2_SALE_SUM ||'</b>';
            return_row_jur_p2.PRICE2_BACK_SUM := '<b>'||return_row_jur_p2.PRICE2_BACK_SUM ||'</b>';
            return_row_jur_p2.PRICE2_SUM := '<b>'||return_row_jur_p2.PRICE2_SUM ||'</b>';
            return_row_jur_p2.DISC_SUM := '<b>'||return_row_jur_p2.DISC_SUM ||'</b>';
            return_row_jur_p2.COST_SUM := '<b>'||return_row_jur_p2.COST_SUM ||'</b>';
            return_row_jur_p2.CNT := '<b>'||return_row_jur_p2.CNT ||'</b>';
            return_row_jur_p2.SALE_CNT := '<b>'||return_row_jur_p2.SALE_CNT ||'</b>';
            return_row_jur_p2.BACK_CNT := '<b>'||return_row_jur_p2.BACK_CNT ||'</b>';
        end if;  
        
        pipe row(return_row_jur_p1);
        if nvl(v_add_period,0) = 1 then
            pipe row(return_row_jur_p2);
        end if;
    end loop;
    
    return_row_total_p1.PRICE2_SALE_SUM := '<b>'||return_row_total_p1.PRICE2_SALE_SUM ||'</b>';
    return_row_total_p1.PRICE2_BACK_SUM := '<b>'||return_row_total_p1.PRICE2_BACK_SUM ||'</b>';
    return_row_total_p1.PRICE2_SUM := '<b>'||return_row_total_p1.PRICE2_SUM ||'</b>';
    return_row_total_p1.DISC_SUM := '<b>'||return_row_total_p1.DISC_SUM ||'</b>';
    return_row_total_p1.COST_SUM := '<b>'||return_row_total_p1.COST_SUM ||'</b>';
    return_row_total_p1.CNT := '<b>'||return_row_total_p1.CNT ||'</b>';
    return_row_total_p1.SALE_CNT := '<b>'||return_row_total_p1.SALE_CNT ||'</b>';
    return_row_total_p1.BACK_CNT := '<b>'||return_row_total_p1.BACK_CNT ||'</b>';
        
    if nvl(v_add_period,0) = 1 then
        return_row_total_p2.PRICE2_SALE_SUM := '<b>'||return_row_total_p2.PRICE2_SALE_SUM ||'</b>';
        return_row_total_p2.PRICE2_BACK_SUM := '<b>'||return_row_total_p2.PRICE2_BACK_SUM ||'</b>';
        return_row_total_p2.PRICE2_SUM := '<b>'||return_row_total_p2.PRICE2_SUM ||'</b>';
        return_row_total_p2.DISC_SUM := '<b>'||return_row_total_p2.DISC_SUM ||'</b>';
        return_row_total_p2.COST_SUM := '<b>'||return_row_total_p2.COST_SUM ||'</b>';
        return_row_total_p2.CNT := '<b>'||return_row_total_p2.CNT ||'</b>';
        return_row_total_p2.SALE_CNT := '<b>'||return_row_total_p2.SALE_CNT ||'</b>';
        return_row_total_p2.BACK_CNT := '<b>'||return_row_total_p2.BACK_CNT ||'</b>';
    end if;   
    
    pipe row(return_row_total_p1);
    if nvl(v_add_period,0) = 1 then   
        pipe row(return_row_total_p2);
    end if;
end sales_by_shops_day;
-------------------------

function sales_send_char(
    d1_1 date,
    d2_1 date,
    d1_2 date,
    d2_2 date,
    v_without_pkg number,
    v_add_period number
) RETURN sales_send_char_t pipelined as

    return_row sales_send_char_r;
begin

    for c1 in (
        select city, pr, city_id
        from
        (
            select 
                case
                    when nvl(v_add_period,0) = 1 then
                        s.name ||' '||d1_1||' - '|| d2_1 
                    else s.name
                end city, 
                round(q.cnt) pr,
                q.city city_id
            from
            (
                select distinct 
                    sum(m.cnt) over(partition by t.sity) cnt, 
                    t.sity city,
                    sum(m.cnt) over() total_cnt 
                from mv_sale1 m
                left join ordh_tp o on m.inet_tp=o.id
                left join trpoint t on m.trpoint=t.id
                where m.cnt>0 
                    and m.part=1 
                    and  m.isinet=1 
                    and (v_without_pkg is null or brand != 66)
                    and o.issite=1
                    and m.dat BETWEEN d1_1 AND d2_1
            ) q
            left join sity s on q.city=s.id
        )
        order by pr desc
    )loop
        return_row.city := c1.city;
        return_row.pr := c1.pr;
        pipe row(return_row);
        
        if nvl(v_add_period,0) = 1 then
            select city, pr
            into return_row.city, return_row.pr
            from
            (
                select 
                    case
                        when nvl(v_add_period,0) = 1 then
                            s.name||' '||d1_2||' - '|| d2_2 
                        else s.name
                    end city,
                    round(q.cnt) pr
                from
                (
                    select distinct 
                        sum(m.cnt) over(partition by t.sity) cnt, 
                        t.sity city,
                        sum(m.cnt) over() total_cnt 
                    from mv_sale1 m
                    left join ordh_tp o on m.inet_tp=o.id
                    left join trpoint t on m.trpoint=t.id
                    where m.cnt>0 
                        and m.part=1 
                        and  m.isinet=1 
                        and (v_without_pkg is null or brand != 66)
                        and o.issite=1
                        and m.dat BETWEEN d1_2 AND d2_2
                        
                ) q
                left join sity s on q.city=s.id
                where q.city = c1.city_id
            );
            pipe row(return_row);
        end if;
    end loop;

end sales_send_char;

end analytic_pkg;