create or replace PACKAGE pay_api AS
    /*
        Перевірка на доступність повторної оплати.
        Запускає перерахунок цін по товарам та записує нові ціни. 
        При успішному перерахунку записуються нові юр. лиця для оплати, при помилці вписується null
    */
    procedure check_repit_pay (
        v_ordh in number
    );

    /*  
        Формування відповіді на перевірку.
        Формат, як відповідь при створенні замовлення.

    */
    PROCEDURE get_response_calc(
        v_ordh in number, 
        v_err out clob
    );
end pay_api;