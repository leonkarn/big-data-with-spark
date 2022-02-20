You will need to have java and also python
Also install the dependencies by using this command
pip install -r requirements.txt

to run the program run this command in your terminal after having installed the depedencies
python pipeline.py -u https://files.grouplens.org/datasets/movielens/ml-100k.zip -z 'ml-100k.zip'

to run the tests run this command in your terminal
pytest py_tests.py
