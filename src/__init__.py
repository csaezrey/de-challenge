import pandas as pd
from params import ENGINE_PATH_WIN_AUTH, CONSOLES_CSV, RESULT_CSV
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, Date, \
    ForeignKey

Base = declarative_base()
engine = create_engine(ENGINE_PATH_WIN_AUTH)
Session = sessionmaker(engine)
session = Session()


def main():
    df_consoles, df_result = initial_load()
    initial_clean(df_consoles, df_result)
    companies = load_companies(df_consoles)
    consoles = load_consoles(df_consoles, companies)
    videogames = load_videogames(df_result)
    load_run_in(df_result, videogames, consoles)
    load_score(df_result, videogames, consoles)


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


def initial_load():
    df_consoles = pd.read_csv(CONSOLES_CSV)
    df_consoles.to_sql(
        'consoles_csv',
        con=engine,
        if_exists='replace',
        schema='staging',
        index=False,
        dtype={'console': String(25), 'company': String(25)},
        )
    df_result = pd.read_csv(RESULT_CSV)
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
            'date': String(25),
            },
        )
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
    inner_join = pd.merge(df_result, df_consoles, how='inner',
                          left_on='console', right_on='console')


def load_companies(df_consoles):
    companies_list = df_consoles['company'].unique()
    companies = {}
    for company in companies_list:
        row = session.query(Company).filter_by(name=company).first()
        if(row is None):
            aux = Company(name=company)
            session.add(aux)
            companies[company] = aux
        else:
            companies[company] = row
    session.commit()
    return companies


def load_consoles(df_consoles, companies):
    consoles_list = df_consoles[['company', 'console']].drop_duplicates()
    consoles = {}
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
    return consoles


def load_videogames(df_result):
    videogames_list = df_result['name'].unique()
    videogames = {}
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
    return videogames


def load_run_in(df_result, videogames, consoles):
    run_in_list = df_result[['name', 'console']].drop_duplicates()
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


def load_score(df_result, videogames, consoles):
    score_list = df_result[['date', 'userscore', 'metascore', 'name',
                            'console']].drop_duplicates()

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

if __name__ == "__main__":
    main()
