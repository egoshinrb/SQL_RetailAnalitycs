-- функция возвращает таблицу для формирование персональных предложений,
-- ориентированных на рост частоты визитов
CREATE OR REPLACE FUNCTION get_offers_frequency_of_visits(
    t_start_date timestamp DEFAULT '2022-01-01', -- первая дата периода
    t_end_date timestamp DEFAULT '2022-12-31', -- последняя дата периода
    add_transactions_count int DEFAULT 1, -- добавляемое число транзакций
    max_churn_rate numeric DEFAULT 100, -- максимальный индекс оттока
    max_discount_share numeric DEFAULT 100, -- максимальная доля транзакций со скидкой (в процентах)
    margin_part numeric DEFAULT 50) -- допустимая доля маржи (в процентах)
RETURNS table
    (customer_id bigint, -- Идентификатор клиента
    start_date timestamp, -- Дата начала периода
    end_date timestamp, -- Дата окончания периода
    required_transactions_count int, -- Целевое количество транзакций
    group_name varchar, -- Группа предложения
    offer_discount_depth int) -- Максимальная глубина скидки
LANGUAGE plpgsql
AS $$
DECLARE
    days_count numeric;
BEGIN
    IF t_start_date > t_end_date THEN
        RAISE EXCEPTION 'ERROR: Дата начала должна быть меньше даты окончания периода';
    END IF;
days_count := get_diff_between_date_in_days(t_end_date, t_start_date);

RETURN QUERY
    SELECT DISTINCT
        g.customer_id,
        t_start_date,
        t_end_date,
        round(days_count / (SELECT customer_frequency FROM customers c WHERE c.customer_id = g.customer_id))::int + add_transactions_count,
        first_value(gs.group_name) OVER w,
        (first_value(g.group_minimum_discount) OVER w * 100)::int / 5 * 5 + 5
    FROM groups g
    JOIN groups_sku gs ON gs.group_id = g.group_id
        AND g.group_churn_rate <= max_churn_rate
        AND g.group_discount_share * 100 < max_discount_share
        AND (g.group_minimum_discount * 100)::int / 5 * 5 + 5
             < (SELECT sum(s2.sku_retail_price - s2.sku_purchase_price) / sum(s2.sku_retail_price)
                FROM sku s
                JOIN stores s2 ON g.group_id = s.group_id
                    AND s.sku_id = s2.sku_id) * margin_part
        WINDOW w as (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC);
END $$;




------------- ТЕСТОВЫЕ ЗАПРОСЫ -------------


SELECT * FROM get_offers_frequency_of_visits();

SELECT * FROM get_offers_frequency_of_visits('2022-08-18 00:00:00', '2022-08-18 00:00:00',
    1,3, 70, 30);

SELECT * FROM get_offers_frequency_of_visits('2022-08-18 00:00:00', '2022-08-18 00:00:00',
    1,10, 50, 50);


