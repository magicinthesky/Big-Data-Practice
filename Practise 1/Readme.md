# Technique: JAVA and Hadoop/MapReduce

Question 1: Find mutual friends from friend lists
Question 2: Find the total number of mutual friends within the Top 10 among all pairs
Question 3: List info of the Top 10 businesses based on the average ratings
Question 4: List the user_id and rating from users that reviewed businesses in 'Palo Alto'


How to run:
Question 1:
  > hadoop jar <question1.jar> <soc-LiveJournal1Adj.txt> <out1>

Question 2:
  > hadoop jar <question2.jar> <soc-LiveJournal1Adj.txt> <out2>

Question 3:
  > hadoop jar <question3.jar> <review.csv> <temp> <business.csv> <out3>
//Note: <temp> is output address from first job which contains <businessid,avgRating>

Question 4:
  > hadoop jar <question4.jar> <review.csv> <business.csv> <out4>
