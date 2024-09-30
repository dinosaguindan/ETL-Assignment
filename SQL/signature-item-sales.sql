select 
    c.customer_id as Customer,
    c.age as Age,
    i.item_name as Item,
    sum(quantity) as Quantity
from customers c
inner join sales s on c.customer_id = s.customer_id
inner join orders o on s.sales_id = o.sales_id
inner join items i on o.item_id = i.item_id
group by c.customer_id ,c.age ,i.item_name
having c.age between 18 and 35