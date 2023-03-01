-- функция возвращает минимальную скидку по группе для каждого клиента
CREATE OR REPLACE FUNCTION get_group_min_discount(t_customer_id BIGINT, t_group_id BIGINT)
RETURNS numeric
LANGUAGE plpgsql
AS $$ BEGIN
    RETURN (
        SELECT
            coalesce(min(c2.sku_discount / c2.sku_summ), 0)
        FROM personal_data p
        JOIN cards c ON p.customer_id = c.customer_id AND p.customer_id = t_customer_id
        JOIN transactions t ON c.customer_card_id = t.customer_card_id
        JOIN checks c2 ON t.transaction_id = c2.transaction_id AND c2.sku_discount > 0
        JOIN sku s ON s.sku_id = c2.sku_id AND s.group_id = t_group_id
        );
END; $$;


-- представление содержит идентификатор покупателя, идентификатор группы товаров, дату первой покупки товара из группы,
-- дату последней покупки товара из группы, количество транзакций с группой, интенсивность покупок группы,
-- минимальный размер скидки по группе
CREATE OR REPLACE VIEW periods as (

-- вспомогательная таблица для расчета показателей: содержит идентификатор покупателя, идентификатор группы товаров,
-- дату первой покупки товара из группы, дату последней покупки товара из группы, количество транзакций с группой
WITH statistics_customers_and_groups as (
    SELECT
        p.customer_id,
        s.group_id,
        min(t.transaction_datetime) as first_group_purchase_date,
        max(t.transaction_datetime) as last_group_purchase_date,
        count(DISTINCT t.transaction_id) as group_purchase
    FROM personal_data p
    JOIN cards c ON p.customer_id = c.customer_id
    JOIN transactions t ON c.customer_card_id = t.customer_card_id
    JOIN checks c2 ON t.transaction_id = c2.transaction_id
    JOIN sku s ON s.sku_id = c2.sku_id
    GROUP BY p.customer_id, s.group_id)

-- выходная таблица
SELECT
    customer_id,
    group_id,
    first_group_purchase_date,
    last_group_purchase_date,
    group_purchase,
    (get_diff_between_date_in_days(first_group_purchase_date, last_group_purchase_date) + 1)
    / group_purchase as group_frequency,
    get_group_min_discount(customer_id, group_id) as group_min_discount
FROM statistics_customers_and_groups
);



----------------- ТЕСТОВЫЕ ЗАПРОСЫ ---------------

SELECT * FROM periods;

SELECT * FROM periods WHERE customer_id = 1;

SELECT * FROM periods WHERE group_frequency > 100;

SELECT * FROM periods WHERE customer_id BETWEEN 100 AND 110;

SELECT * FROM periods WHERE group_purchase > 7;