SELECT 
* FROM 
{{ source('ecomm', 'orders_au_duped') }}
ORDEr BY total_amount