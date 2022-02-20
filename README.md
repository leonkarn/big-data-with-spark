# BIG DATA ASSIGNMENT

You will need to have java and also python
Also install the dependencies by using this command
~~~
pip install -r requirements.txt
~~~
To run the program run this command in your terminal after having installed the dependencies

~~~
python pipeline.py -u https://files.grouplens.org/datasets/movielens/ml-100k.zip -z 'ml-100k.zip'
~~~

To run the tests run this command in your terminal

~~~
pytest py_tests.py
~~~


### Staging

The results were saved in `delta lake` folder


### Transformation

The calculations were saved in `results` folder 