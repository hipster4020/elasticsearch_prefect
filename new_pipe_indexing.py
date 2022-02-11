import math
import time
from datetime import datetime
from re import I
from typing import Any
from urllib.parse import quote

import pandas as pd
import prefect
from dateutil import relativedelta
from elasticsearch import Elasticsearch, helpers
from prefect import Flow, task
from sqlalchemy import create_engine
from tqdm import tqdm


def get_connection():
    engine = create_engine(
        "mysql://id:%s@ip:port/db" % quote("pwd")
    )
    return engine


def filterKeys(document, keys):
    """[summary]
        document에서 keys에 해당하는 딕션너리를 만들어서 반환
    Args:
        document ([dict]): [dictionary 객체]
        keys ([list of str]): [dictionary 키 리스트]

    Returns:
        [dictionary]: [document에서 keys에 해당하는 키만 모아서 딕션너리로 반환]
    """
    return {key: document[key] for key in keys}


def insert_doc_generator(df, keys, index_name, primary_key=None):
    """[summary]
        df에서 데이터를 색인용 데이터를 생성
    Args:
        df ([pandas.DataFrame]): []
        keys ([list of str]): [index에서 사용할 키 리스트]
        index_name ([str]): [index 명칭]
        primary_key ([str], optional): [index의 id에 해당하는 필드 명]

    Yields:
        [dictionary]: [elasticsearch index type]
    """

    df_iter = df.iterrows()
    for index, document in df_iter:
        if primary_key:
            yield {
                "_index": index_name,
                "_id": f"{document[primary_key]}",
                "_source": filterKeys(document, keys),
            }
        else:
            yield {
                "_index": index_name,
                "_source": filterKeys(document, keys),
            }


def update_doc_generator(
    df: pd.DataFrame, keys: list[Any], index_name: str, primary_key: str
):

    """[summary]
        df에서 데이터를 색인용 데이터를 생성
    Args:
        df ([pandas.DataFrame]): []
        keys ([list of str]): [index에서 사용할 키 리스트]
        index_name ([str]): [index 명칭]
        primary_key ([str]): [index의 id에 해당하는 필드 명]

    Yields:
        [dictionary]: [elasticsearch index type]
    """

    if primary_key not in df.columns:
        raise Exception("key not in dataframe")

    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
            "_index": index_name,
            "_op_type": "update",
            "_id": f"{document[primary_key]}",
            "doc": filterKeys(document, keys),
        }


@task
def segments_insert_parallel(
    es, df, keys, index_name, fn_doc_generator, primary_key=None, batch=4000, offset=0
):
    """[summary]
    es에 df의 데이터를 색임하는 함수

    Args:
        es ([ElasticSearch]): []
        df ([type]): [description]
        keys ([type]): [description]
        index_name ([type]): [description]
        fn_doc_generator([function of generator]) : doc generator
        primary_key ([type], optional): [description]. Defaults to None.
        batch (int, optional): [description]. Defaults to 250.
        offset (int, optional): [description]. Defaults to 0.
    """
    total = math.ceil(df.shape[0] / batch)
    retry_count = 0
    sub_retry_count = 0
    relative_time_gap = 0
    logger = prefect.context.get("logger")

    pbar = tqdm(range(total))
    for offset in pbar:
        try:
            if sub_retry_count > 0:
                print(f"retry offset : {offset}")

            data = df.iloc[offset * batch : (offset + 1) * batch]
            for success, info in helpers.parallel_bulk(
                es,
                fn_doc_generator(data, keys, index_name, primary_key),
                thread_count=6,
                chunk_size=1000,
                queue_size=16,
            ):
                if not success:
                    logger.info("A document failed to index: {}".format(info))

            relative_time_gap = int((offset + 1) / 100)
            sub_retry_count = 0

            if offset > 0 and (offset % 10) == 0:
                time.sleep(relative_time_gap + 2)

            if offset > 0 and (offset % 100) == 0:
                time.sleep(relative_time_gap + 3)

        except Exception as e:
            sub_retry_count += 1
            retry_count += 1
            print(f"retry {sub_retry_count}/{retry_count} with offset : {offset}")
            time.sleep(1 * sub_retry_count + relative_time_gap)
            print(e)
            continue
        finally:
            del data


keys = [
    "column1",
    "column2",
    "column3",
    "column4",
    "column5",
    "column6",
    "column7",
    "column8",
]
primary_key = "id"


@task
def get_index_count(index_name) -> int:
    logger = prefect.context.get("logger")
    logger.info("get_index_count")

    es = Elasticsearch("dev-server:9200")
    es.indices.refresh(index_name)
    res = es.cat.count(index_name, params={"format": "json"})
    index_count = int(res[0]["count"])

    logger.info(f"index count:{index_count}")
    return index_count


@task
def get_last_indexing_docid(index_name) -> str:
    logger = prefect.context.get("logger")
    logger.info("extract_reference_data")
    try:
        es = Elasticsearch("dev-server:9200")
        query = {
            "_source": [""],
            "size": 1,
            "sort": [{"create_date": "desc"}],
            "query": {"match_all": {}},
        }
        response = es.search(index=index_name, body=query)
        es.transport.connection_pool.close()
        hits = response["hits"]
        last_doc_id = hits["hits"][0]["_id"]
        return last_doc_id
    except Exception as e:
        print(f"error{e}")
        return ""


@task
def extract_alldata() -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("extract_data_with_lastid")

    query = f"select * from indexing_news order by id "
    engine = get_connection()
    live_data = pd.read_sql(query, engine)
    engine.dispose()
    return live_data


@task
def extract_data_with_lastid(reference_data) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("extract_data_with_lastid")

    query = f"select * from indexing_news where id > {reference_data} "
    engine = get_connection()
    live_data = pd.read_sql(query, engine)
    engine.dispose()
    return live_data


@task
def extract_data_after_yester_day_updated() -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("extract_data_after_yester_day_updated")
    yesterday = datetime.now().date()
    yesterday = yesterday - relativedelta.relativedelta(days=1)

    query = f"select * from indexing_news where updated > {datetime.strftime(yesterday, '%Y-%m-%d')} "
    engine = get_connection()
    live_data = pd.read_sql(query, engine)
    engine.dispose()
    return live_data


@task
def transform(live_data):
    logger = prefect.context.get("logger")
    logger.info("transform data")

    # clean the live data
    live_data.fillna("", inplace=True)
    live_data["column1"] = live_data["keyword"].map(
        lambda row: [element.strip() for element in row.split(",") if element.strip()]
    )
    live_data["column3"] = live_data["portal"].map(
        lambda row: [element.strip() for element in row.split(":") if element.strip()]
    )
    live_data["column7"] = live_data["predict"].map(
        lambda row: [element.strip() for element in row.split(",") if element.strip()]
    )
    return live_data


@task
def data_indexing(indexing_data: pd.DataFrame, update_data: pd.DataFrame, index_name):
    logger = prefect.context.get("logger")
    logger.info("data_indexing data")

    logger.info("connect to es cloud")
    es = Elasticsearch("dev-server:9200")

    logger.info(f"index data  count {indexing_data.shape}")
    logger.info(f"update data  count {update_data.shape}")

    logger.info("indexing start")
    if len(indexing_data) > 0:
        segments_insert_parallel.run(
            es, indexing_data, keys, index_name, insert_doc_generator, primary_key
        )

    if len(update_data) > 0:
        segments_insert_parallel.run(
            es, update_data, keys, index_name, update_doc_generator, primary_key
        )

    logger.info("indexing complete")
    es.indices.refresh(index_name)
    es.transport.connection_pool.close()


@task
def full_index(index_name):
    logger = prefect.context.get("logger")
    logger.info("start with full index")

    query = "select count(*) from indexing_news "

    engine = get_connection()
    row = engine.execute(query).fetchall()
    db_count = row[0][0]
    logger.info(f" db count : {db_count}")

    batch_size = 100000
    total = math.ceil(db_count / batch_size)
    pbar = tqdm(range(total))
    for index in pbar:
        logger.info(f"indexing : {index}/{total}")
        batch_query = f"select * from indexing_news limit {index*batch_size}, 100000"
        live_data = pd.read_sql(batch_query, engine)
        live_data = transform.run(live_data)
        update_data = pd.DataFrame()
        data_indexing.run(live_data, update_data, index_name)
        del live_data

    engine.dispose()


@task
def increment_index(index_name):
    logger = prefect.context.get("logger")
    logger.info("start increment indexing")
    reference_data = get_last_indexing_docid.run(index_name)
    insert_data = extract_data_with_lastid.run(reference_data)
    insert_data = transform.run(insert_data)

    logger.info("generate update data")
    update_data = extract_data_after_yester_day_updated.run()
    update_data = transform.run(update_data)
    data_indexing.run(insert_data, update_data, index_name)
    del insert_data
    del update_data


@task
def daily_indexing(index_name):
    logger = prefect.context.get("logger")

    index_count = get_index_count.run(index_name)
    logger.info(f"{type(index_count)}, {index_count}")

    if index_count > 0:
        logger.info("increment_index")
        increment_index.run(index_name)
    else:
        logger.info("full_index")
        full_index.run(index_name)


def main():
    logger = prefect.context.get("logger")
    logger.info("starting indexing ")

    with Flow("news_data_indexing_local") as flow:
        logger.info("load all data")
        index_name = "index_v5"
        daily_indexing(index_name)
        
    flow.register("network")


if __name__ == "__main__":
    main()
