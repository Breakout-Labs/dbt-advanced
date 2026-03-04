select 'ordered' as order_status, 'Ordered' as order_status_normalized
union all
select 'order_created' as order_status, 'Ordered' as order_status_normalized
union all
select 'shipped' as order_status, 'Shipped' as order_status_normalized
union all
select 'sent' as order_status, 'Shipped' as order_status_normalized
union all
select 'pending' as order_status, 'Pending' as order_status_normalized
union all
select 'waiting' as order_status, 'Pending' as order_status_normalized
union all
select 'processing' as order_status, 'Pending' as order_status_normalized
union all
select 'payment_pending' as order_status, 'Pending' as order_status_normalized
union all
select 'canceled' as order_status, 'Canceled' as order_status_normalized
union all
select 'cancelled' as order_status, 'Canceled' as order_status_normalized
union all
select 'delivered' as order_status, 'Delivered' as order_status_normalized
