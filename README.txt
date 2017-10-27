Hi there, non-existent reader! This one was a College project of mine. 
In short, it is an Artificial Neural Network that can predict the final destination(pun not intended!) of a cab from the partial trajectory,
and some other meta data about the trip.
I wanted to do a project on Large scale Machine Learning using Hadoop. So we (i.e. me & my mentor) hunted around & finally came across a
Kaggle dataset containing one year's worth of taxi trip data from Porto, Portugal. We were too late to participate in the Kaggle Challenge,
but the data came in useful.
AT first we experimented with clustering based approach. However the results weren't very promising. Plus most ML works in the geospatial 
domain seem to use CLustering, & I wanted to do something a bit different.
SO I went ahead with a Multi Layer Perceptron model with Backpropagation & batch gradient descent, with just one Hidden Layer. 
Implementation was done folllowing an approach suggested by Chu et al ("Mapreduce for Machine Learning on Multicore", 2007) that works well
with Hadoop architecture. The model had only 1 Hidden Layer, but even then each iteration took a looottt of time 
on our cluster(5 compute nodes & 1 server node). 
We experimented with different combination of Activation functions & error functions. In the best SIgmoid activation-L1 error yielded the best results.
One final point, I am aware that the Hadoop ecosystem has many ML tools which could have made implementing a solution like this quicker & perhaps
more efficient. But I still chose to code the whole thing from the bottom up without using any dedicated ML, Linear ALgebra or numerical methods
libraries. This was because my Mentor wanted me to do this the old fashioned way, by getting my hands dirty. And I am very grateful to him
for that. Because I am sure that had I just used one of the many excellent libraries or tools available, my understanding of Neural Nets would 
have been poorer(not that it's particularly great, even now...) On the other hand this also meant that I discovered a small but VERY
significant error in the Backpropagation code in the absolute last stage of the project, & had to stay up nights, fixing & re-training
the Net, but that's a tale for a different day......

Anyways, feel free to check out the codes . Open to all suggestions. 
NN1red is the training code, NNtest841 is the testing code(no, i don't remember why I named them like this).
WIll upload the data preprocessing codes as well, if only I can find them.
