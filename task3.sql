--1
SELECT category.name, COUNT(film_category.film_id)AS amount FROM category 
JOIN film_category ON category.category_id = film_category.category_id
GROUP BY category.name
ORDER BY amount DESC;

--2
SELECT actor.first_name, actor.last_name, SUM(film.rental_duration)AS total_rental_duration FROM actor
JOIN film_actor USING(actor_id)
JOIN film USING(film_id)
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 10;

--3
SELECT category.name, SUM(payment.amount) AS payment_amount FROM category
JOIN film_category USING(category_id)
JOIN film USING(film_id)
JOIN inventory USING(film_id)
JOIN rental USING(inventory_id)
JOIN payment USING(rental_id)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

--4
SELECT  film.title FROM film
LEFT JOIN inventory USING(film_id)
GROUP BY film.film_id
HAVING COUNT(inventory.inventory_id) = 0;

--5
SELECT actor.first_name, actor.last_name,COUNT(film.film_id) AS quantity FROM category
JOIN film_category USING(category_id)
JOIN film USING(film_id)
JOIN film_actor USING(film_id)
JOIN actor USING(actor_id)
WHERE category.name = 'Children'
GROUP BY 1,2
HAVING COUNT(film.film_id) IN (SELECT DISTINCT(COUNT(film.film_id)) FROM category
								JOIN film_category USING(category_id)
								JOIN film USING(film_id)
								JOIN film_actor USING(film_id)
								JOIN actor USING(actor_id)
								WHERE category.name = 'Children'
								GROUP BY actor.actor_id
								ORDER BY 1 DESC
							  	LIMIT 3)
ORDER BY 3 DESC;

--6
SELECT city.city, COUNT(customer.active)AS active_quantity,
	SUM(ABS( customer.active-1))AS inactive_quantity FROM customer 
JOIN address USING(address_id)
JOIN city USING(city_id)
GROUP BY city.city_id
ORDER BY 3 DESC;

--7
(SELECT  category.name , SUM(rental.return_date - rental.rental_date)  FROM city
JOIN address USING(city_id)
JOIN customer USING(address_id)
JOIN rental USING(customer_id)
JOIN inventory USING(inventory_id)
JOIN film USING(film_id)
JOIN film_category USING(film_id)
JOIN category USING(category_id)
WHERE( city.city LIKE 'A%' OR city.city LIKE 'a%') AND rental.return_date IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1)
UNION ALL
(SELECT  category.name , SUM(rental.return_date - rental.rental_date)  FROM city
JOIN address USING(city_id)
JOIN customer USING(address_id)
JOIN rental USING(customer_id)
JOIN inventory USING(inventory_id)
JOIN film USING(film_id)
JOIN film_category USING(film_id)
JOIN category USING(category_id)
WHERE city.city LIKE '%-%' AND rental.return_date IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1);

