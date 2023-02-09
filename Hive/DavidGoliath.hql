select case
when winner = 'black' and black_rating > white_rating then 'Goliath'
when winner = 'white' and white_rating > black_rating then 'Goliath'
else 'David' END as dog, count(id)
from game
group by case
when winner = 'black' and black_rating > white_rating then 'Goliath'
when winner = 'white' and white_rating > black_rating then 'Goliath'
else 'David' END;