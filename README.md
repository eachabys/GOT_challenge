# GOT_challenge

![Context](https://github.com/eachabys/GOT_challenge#Context)

![Documents](https://github.com/eachabys/GOT_challenge#Documents)

![Output](https://github.com/eachabys/GOT_challenge#Output)

![Instructions](https://github.com/eachabys/GOT_challenge#Instructions)

### Context
Aemon the Castle Black’s maester is a close advisor of Jeor Mormont, the Lord Comman-
der. He is also the Castle Black’s library director. This library has hundreds of thousands
of books and some of them are so rare you can not even find them in the Citadel.
Searching information in this huge books collection is getting more and more challeng-
ing for Aemon as the years pass. He decided to ask Sam, his favorite trainee for some
help to find a way to easily search information in documents from keywords. Then
Sam asked his friend Jon Snow who always liked information search and data engineering.
Jon Snow decided to use vector representation of documents. Search engines use dif-
ferent kind of document representations and one of them is the vector representation.
It gives the ability to directly use mathematical tools such as distance, similarity and
dimension reduction.
An efficient implementation will build an inverted index of a large collec-
tion of documents.

### Documents
The library contains a collection of N documents. Some of the documents from the collection can be found ![here](https://github.com/Samariya57/coding_challenges/tree/master/data/indexing).

### Output 

##### Dictionary
The solution to the problem outputs a dictionary that matches every word from the documents with a unique id.

##### Inverted index
Using both the dataset and the dictionnary the soluton provides a list of inverted indexes for
every word.

### Instructions
The solution was implemented in pyspark (python v2.17.12)
