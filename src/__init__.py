import pandas as pd
import logging
import sys
from datetime import date
from params import ENGINE_PATH_WIN_AUTH, CONSOLES_CSV, RESULT_CSV, ENABLED_DEBUG
from messages import ERROR_LOAD_SCORE, ERROR_LOAD_RELATIONSHIP,\
    ERROR_LOAD_VIDEOGAME, ERROR_LOAD_CONSOLES, ERROR_LOAD_COMPANIES,\
    ERROR_READING_CSV_CONSOLES, ERROR_READING_CSV_RESULT,\
    START_PROCESS, START_LOAD_COMPANIES, START_LOAD_CONSOLES,\
    START_LOAD_VIDEOGAMES, START_LOAD_RELATION_TABLE, START_LOAD_SCORE,\
    END_PROCESS
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, Date,\
    ForeignKey

Base = declarative_base()
engine = create_engine(ENGINE_PATH_WIN_AUTH)
Session = sessionmaker(engine)
session = Session()


def main():
    if(ENABLED_DEBUG):
        start_log()
    logging.info(START_PROCESS)
    df_consoles, df_result = initial_load()
    initial_clean(df_consoles, df_result)
    logging.info(START_LOAD_COMPANIES)
    companies = load_companies(df_consoles)
    logging.info(START_LOAD_CONSOLES)
    consoles = load_consoles(df_consoles, companies)
    logging.info(START_LOAD_VIDEOGAMES)
    videogames = load_videogames(df_result)
    logging.info(START_LOAD_RELATION_TABLE)
    load_run_in(df_result, videogames, consoles)
    logging.info(START_LOAD_SCORE)
    load_score(df_result, videogames, consoles)
    session.close()
    logging.info(END_PROCESS)

class Company(Base):
    __tablename__ = 'company'
    __table_args__ = {'schema': 'usr_score'}
    name = Column(String(25), nullable=False)
    company_id = Column(Integer(), primary_key=True)

    def __str__(self):
        return self.name


class Console(Base):
    __tablename__ = 'console'
    __table_args__ = {'schema': 'usr_score'}
    name = Column(String(25), nullable=False)
    console_id = Column(Integer(), primary_key=True)
    company_id = Column(Integer(), ForeignKey(Company.company_id),
                        nullable=False)

    def __str__(self):
        return self.name


class Videogame(Base):
    __tablename__ = 'videogame'
    __table_args__ = {'schema': 'usr_score'}
    name = Column(String(120), nullable=False)
    videogame_id = Column(Integer(), primary_key=True)

    def __str__(self):
        return self.name


class RunIn(Base):
    __tablename__ = 'run_in'
    __table_args__ = {'schema': 'usr_score'}
    videogame_id = Column(Integer(), ForeignKey(Videogame.videogame_id),
                          primary_key=True)
    console_id = Column(Integer(), ForeignKey(Console.console_id),
                        primary_key=True)

    def __str__(self):
        return self.name


class Score(Base):

    __tablename__ = 'score'
    __table_args__ = {'schema': 'usr_score'}
    videogame_id = Column(Integer(),
                          ForeignKey(RunIn.videogame_id),
                          primary_key=True)
    console_id = Column(Integer(),
                        ForeignKey(RunIn.console_id),
                        primary_key=True)
    registration_date = Column(Date(), primary_key=True)
    userscore = Column(Numeric(2, 1), nullable=False)
    metascore = Column(Integer(), nullable=False)

    def __str__(self):
        return self.name

def start_log():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

def initial_load():
    try:
        df_consoles = pd.read_csv(CONSOLES_CSV)
        df_consoles['created_date'] = date.today()
        df_consoles.to_sql(
            'consoles_csv',
            con=engine,
            if_exists='replace',
            schema='staging',
            index=False,
            dtype={'console': String(25), 'company': String(25)},
            )
    except IOError as e:
        logging.error(ERROR_READING_CSV_CONSOLES)
    try:
        df_result = pd.read_csv(RESULT_CSV)
        df_result['created_date'] = date.today()
        df_result.head().to_sql(
            'result_csv',
            con=engine,
            if_exists='replace',
            schema='staging',
            index=False,
            dtype={
                'metascore': String(3),
                'name': String(125),
                'console': String(25),
                'userscore': String(3),
                'date': String(25)
                },
            )
    except IOError as e:
        logging.error(ERROR_READING_CSV_RESULT)
    return df_consoles, df_result


def initial_clean(df_consoles, df_result):
    # Limpieza de datos
    df_consoles['console'] = df_consoles['console'].map(lambda x:
                                                        x.strip().upper())
    df_consoles['company'] = df_consoles['company'].map(lambda x:
                                                        x.strip().upper())
    df_result['date'] = pd.to_datetime(df_result['date'])
    df_result['userscore'] = pd.to_numeric(df_result['userscore'],
                                           errors='ignore')
    df_result['name'] = df_result['name'].str.strip()
    df_result['console'] = df_result['console'].map(lambda x:
                                                    x.strip().upper())


def load_companies(df_consoles):
    companies_list = df_consoles['company'].unique()
    companies = {}
    try:
        for company in companies_list:
            row = session.query(Company).filter_by(name=company).first()
            if(row is None):
                aux = Company(name=company)
                session.add(aux)
                companies[company] = aux
            else:
                companies[company] = row
        session.commit()
    except:
        session.rollback()
        logging.error(ERROR_LOAD_COMPANIES)
    return companies


def load_consoles(df_consoles, companies):
    consoles_list = df_consoles[['company', 'console']].drop_duplicates()
    consoles = {}
    try:
        for (index, console) in consoles_list.iterrows():
            row = session.query(Console).filter_by(
                                            name=console['console'],
                                            company_id=companies[console['company']].company_id
                                          ).first()
            if(row is None):
                aux = Console(name=console['console'],
                              company_id=companies[console['company']].company_id)
                session.add(aux)
                consoles[console['console']] = aux
            else:
                consoles[console['console']] = row
        session.commit()
    except:
        session.rollback()
        logging.error(ERROR_LOAD_CONSOLES)
    return consoles


def load_videogames(df_result):
    videogames_list = df_result['name'].unique()
    videogames = {}
    try:
        for videogame in videogames_list:
            row = session.query(Videogame).filter_by(
                                            name=videogame
                                          ).first()
            if(row is None):
                aux = Videogame(name=videogame)
                session.add(aux)
                videogames[videogame] = aux
            else:
                videogames[videogame] = row
        session.commit()
    except:
        session.rollback()
        logging.error(ERROR_LOAD_VIDEOGAME)
    return videogames


def load_run_in(df_result, videogames, consoles):
    run_in_list = df_result[['name', 'console']].drop_duplicates()
    try:
        for (index, item) in run_in_list.iterrows():
            row = session.query(RunIn).filter_by(
                                            console_id=consoles[item['console']].console_id,
                                            videogame_id=videogames[item['name']].videogame_id,
                                          ).first()
            if(row is None):
                aux = RunIn(console_id=consoles[item['console']].console_id,
                            videogame_id=videogames[item['name']].videogame_id)
                session.add(aux)
        session.commit()
    except:
        session.rollback()
        logging.error(ERROR_LOAD_RELATIONSHIP)


def load_score(df_result, videogames, consoles):
    score_list = df_result[['date', 'userscore', 'metascore', 'name',
                            'console']].drop_duplicates()
    try:
        for index, score in score_list.iterrows():
            row = session.query(Score).filter_by(
                                            console_id=consoles[score['console']].console_id,
                                            videogame_id=videogames[score['name']].videogame_id,
                                            registration_date=score['date']
                                          ).first()
            if (score['userscore'] == 'tbd'):
                aux_score = None
            else:
                aux_score = pd.to_numeric(score['userscore'], errors='ignore') + 0.0
            if(row is None):
                aux = Score(registration_date=score['date'],
                            userscore=aux_score,
                            metascore=score['metascore'],
                            videogame_id=videogames[score['name']].videogame_id,
                            console_id=consoles[score['console']].console_id
                            )
                session.add(aux)
            else:
                row.userscore = aux_score
                row.metascore = score['metascore']
        session.commit()
    except:
        session.rollback()
        logging.error(ERROR_LOAD_SCORE)

if __name__ == "__main__":
    main()
