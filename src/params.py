DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
USERNAME = 'system'
PASSWORD = 'oracle'
HOST = 'localhost'
PORT = 1521
SERVICE = 'xe'
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME \
    + ':' + PASSWORD + '@' + HOST + ':' + str(PORT) + '/?service_name=' \
    + SERVICE

PATH = "/home/data/"
CONSOLES_CSV = PATH + 'consoles.csv'
RESULT_CSV = PATH + 'result.csv'
ENABLED_DEBUG = True