-- представление содержит идентификатор покупателя, идентификатор транзакции, дата и время транзакции, группа товаров,
-- себестоимость купленных товаров, базовая розничная стоимость, фактически оплаченная стоимость
CREATE OR REPLACE VIEW purchase_history as (
SELECT DISTINCT
    p.customer_id,
    t.transaction_id,
    t.transaction_datetime,
    sku.group_id,
    sum(c2.sku_amount * s.sku_purchase_price) OVER w1 as group_cost,
    sum(c2.sku_summ) OVER w1 as group_summ,
    sum(c2.sku_summ_paid) OVER w1 as group_summ_paid
FROM personal_data p
    JOIN cards c ON p.customer_id = c.customer_id
    JOIN transactions t ON c.customer_card_id = t.customer_card_id AND t.transaction_datetime <= get_date_analysis()
    JOIN checks c2 ON t.transaction_id = c2.transaction_id
    JOIN sku ON sku.sku_id = c2.sku_id
    JOIN stores s ON sku.sku_id = s.sku_id AND t.transaction_store_id = s.transaction_store_id
WINDOW w1 as (PARTITION BY p.customer_id, t.transaction_id, transaction_datetime, sku.group_id)
);



-------------- ТЕСТОВЫЕ ЗАПРОСЫ ----------------

SELECT * FROM purchase_history;

SELECT * FROM purchase_history WHERE customer_id = 5 and group_id = 7;

SELECT * FROM purchase_history WHERE transaction_datetime BETWEEN '2022-05-15' AND '2022-05-16';

SELECT * FROM purchase_history WHERE group_summ_paid / group_summ < 0.6;

SELECT * FROM purchase_history WHERE customer_id = 11;

