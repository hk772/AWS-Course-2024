Students:
    Hagai Kordonsky 316119668  חגי קורדונסקי
    Gal Pinto       211564265   גל פינטו


How to Run:
	copy the credentials as is into following files:
		~\.aws\config
		~\.aws\config
	
	running the main of AWSApp class will run out jar with the 3grams input


Implementation details:

	Job0
pre requisites:
	- the stpwords file in the s3 filesystem, we pass the files location as a parameter to the config

Mapper
	setup -> loading the stopwords file into memory as a map

	map -> gets the lines from the 3grams: "w1 w2 w3 _ m_c ..."
		->  filters 3grams containing stop words
		->  does 2 emits, 1 for w1,w2   and 1 for w2,w3   sends the other word , and matchcount, as value
			the value will have a tag   0 for regular, and 1 for reversed

Reducer
	setup -> created outputstream for single words file

	reduce -> will receive other word, and m_c grouped by the word pair in the key
		-> creating a hashmap that will count for each other word a value for it 
				reversed appearances m_c
				and regular appearances m_c		
			(both counts needed to be able to separate the count of the w1,w2,w3 3gram and the w3,w1,w2)
			(and for not counting twice the middle word for single word count)
		-> simultaneously counting the total count of the word pair in the key
		-> after going over all input records, write singles and their counts
		-> and for each word, emit both possible 3grams with relevant counts



	Job1

inputs: output of job0 and the singleCount filles we written from job0

Mapper:
	map -> classifies input 
		input from singleCounts:
			-> emits the count as value, and for the key the word, and a single tag so it will come first at the reducer  
				(tag comparison implemented in the compareTo, in our custom key)
		input from out0
			-> contains the <w1,w2,w3,count123,count12,count23>
					where between count12 and count23, one of them is zero and the other contains the actual word pair count
				(for each 3gram we will receive 2 records, 1 with count12 (regular tag from previous job) and 1 for count23 (counted at reversed tag))
			-> we will use only the record where count12 > 0
			-> emit 3 pairs, 1 for each word in the 3gram, key will have 3gram tag in order for them to come after the singletag, 
				value will be the 3gram, the counts, and a tag to know if it was the first second or third word in the 3gram

Reducer
	reduce -> gets all the records grouped by a word
		-> for each word we expect to receive 2 consecutive calls to the reducer:	
			-> first will be all the single word counts (grouped by the word): sum them to get the total word count
			->second will be all the 3grams containing this word
				we already calculated the wordcount so we emit 
						 w1,w2,w3,count123,count12,count23,count2,count3
					count2 or 3 will be the current word counted based on the tag and the other one 0

we emitted 2 records with the same 3gram where count23 == 0, and each one containing 1 piece of info about count2 or count3

	JobC0:
gets as input the single words count files we written in job0
count the total amount of words and writes it as output

	job2:
receives as config parameter the location of JobC0 output
inputs are from job0 and job1

Mapper
	map -> classifies between ou0 lines and out1 lines
		-> if out0 line: only emits if count23 >0, the other line is accounted for in the out1 files
			key: w1,w2
			val: w3, count123, count12, count23, 0, 0
		-> if out1 line
			key: w1,w2
			val: w3, count123, count12, count23, count2, count3

Reducer:
	setup -> loads C0 from config parameter file path

	reduce -> maintain hashmap<w3, [count123, count12, count23, count2 count3]>
		for each value add values to the hashmap array

		after adding all data for wordpair key:
		calculate the probs for each maop entry:
			count12 wa counted twice bc of out1 so devide by 2 in the calculation
			save the probs in a sorted map
		
		emit all data from sorted map (in reverse order)
		key: w1,w2,prob
		val: w3

outputformat:		correcting order
	write(w1,w2,w3,prob)



