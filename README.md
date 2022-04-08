# BIG DATA 

You will need to have java and also python

Also install the dependencies by using this command
~~~
pip install -r requirements.txt
~~~
To run the program run this command in your terminal after having installed the dependencies

~~~
python pipeline.py -u https://files.grouplens.org/datasets/movielens/ml-100k.zip -z 'ml-100k.zip'
~~~

To run the tests run this command in your terminal (Run tests only after having run the program with the command above)

~~~
pytest py_tests.py
~~~
## Structure

There are two classes: movie and rating. Each of them is responsible for data about movie and rating respectively.

Pipeline is our main function.

### Staging

The ingested data are saved in `delta lake` folder


### Transformation

The transformed data are saved in `results` folder 
