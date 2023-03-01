-- функция возвращает таблицу для формирование персональных предложений,
-- ориентированных на кросс-продажи
CREATE OR REPLACE FUNCTION get_offers_cross_sales(
        groups_count int DEFAULT 1, -- количество групп
        max_churn_rate numeric DEFAULT 10, -- максимальный индекс оттока
        max_stability_index numeric DEFAULT 10, -- максимальный индекс стабильности потребления
        max_part_sku numeric DEFAULT 100, -- максимальная доля SKU (в процентах)
        margin_part numeric DEFAULT 50) -- допустимая доля маржи (в процентах)
RETURNS table(
    customer_id bigint,
    sku_name varchar,
    offer_discount_depth int)
LANGUAGE plpgsql
AS $$ BEGIN
RETURN QUERY
WITH get_customers_and_groups as (
    SELECT
        g.customer_id,
        g.group_id,
        (g.group_minimum_discount * 100)::int / 5 * 5 + 5 as group_minimum_discount,
        row_number() OVER w as rank_affinity
    FROM groups g
    WHERE g.group_churn_rate <= max_churn_rate
        AND g.group_stability_index < max_stability_index
    WINDOW w as (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC)),

add_margin_and_sku_id as (
    SELECT
        gc.customer_id,
        gc.group_id,
        gc.group_minimum_discount,
        s.sku_retail_price - s.sku_purchase_price as margin,
        row_number() OVER w1 as rank_margin,
        s.sku_id,
        c.customer_primary_store
    FROM get_customers_and_groups gc
    JOIN customers c ON gc.rank_affinity <= groups_count
        AND gc.customer_id = c.customer_id
    JOIN stores s ON c.customer_primary_store = s.transaction_store_id
    JOIN sku s2 ON gc.group_id = s2.group_id
        AND s.sku_id = s2.sku_id -- возможно лишнее
    WINDOW w1 as (PARTITION BY gc.customer_id, gc.group_id
        ORDER BY s.sku_retail_price - s.sku_purchase_price DESC)),

add_part_sku_in_groups as (
    SELECT
        a1.customer_id,
        a1.group_id,
        a1.sku_id,
        (SELECT count(DISTINCT c2.transaction_id) FROM purchase_history ph
        JOIN checks c2 ON ph.customer_id = a1.customer_id
            AND ph.group_id = a1.group_id
            AND ph.transaction_id = c2.transaction_id
            AND c2.sku_id = a1.sku_id)::numeric
        / (SELECT p.group_purchase FROM periods p
            WHERE p.customer_id = a1.customer_id
              AND p.group_id = a1.group_id) as part_sku_in_groups,
        a1.group_minimum_discount,
        a1.customer_primary_store
    FROM add_margin_and_sku_id a1
    WHERE rank_margin = 1),

get_allowable_discount as (
    SELECT
        a2.customer_id,
        a2.group_id,
        a2.sku_id,
        a2.group_minimum_discount,
        (SELECT sum(s.sku_retail_price - s.sku_purchase_price) / sum(s.sku_retail_price) * margin_part
         FROM stores s WHERE s.transaction_store_id = a2.customer_primary_store) as allowable_discount
    FROM add_part_sku_in_groups a2
    WHERE a2.part_sku_in_groups * 100 <= max_part_sku)

SELECT
    ga.customer_id,
    s.sku_name,
    ga.group_minimum_discount
FROM get_allowable_discount ga
JOIN sku s ON ga.sku_id = s.sku_id
WHERE ga.group_minimum_discount <= ga.allowable_discount;
END $$;




--------------- ТЕСТОВЫЕ ЗАПРОСЫ -----------------

SELECT * FROM get_offers_cross_sales();

SELECT * FROM get_offers_cross_sales(5, 3, 0.5,
    100, 30);

SELECT * FROM get_offers_cross_sales(5, 3, 0.5,
    100, 50);