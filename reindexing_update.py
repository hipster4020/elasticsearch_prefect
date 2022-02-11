import math
import time
from typing import Any

import pandas as pd
import pymysql
from elasticsearch import Elasticsearch, helpers
from sqlalchemy import create_engine
from tqdm import tqdm


def transform(live_data):
    # clean the live data
    live_data.fillna("", inplace=True)
    live_data["predict"] = live_data["predict"].map(
        lambda row: [element.strip() for element in row.split(",") if element.strip()]
    )
    return live_data


def db_load():
    print("dataload start")
    pymysql.install_as_MySQLdb()

    engine = create_engine(
        "mysql://id:pwd@ip:port/db"
    )
    df = pd.read_sql(
        "select id, predict from indexing_news limit 100",
        engine,
    )
    df = transform(df)
    print("dataload end")

    return df


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


def update_doc_generator(df, keys, index_name, primary_key):

    """[summary]
        df에서 데이터를 색인용 데이터 update
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


def segments_insert_parallel(
    es, df, keys, index_name, fn_doc_generator, primary_key=None, batch=4000, offset=0
):
    """[summary]
    es에 df의 데이터를 색인하는 함수

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
                    print("A document failed to index: {}".format(info))

            relative_time_gap = int((offset + 1) / 100)
            sub_retry_count = 0

            if offset > 0 and (offset % 10) == 0:
                time.sleep(relative_time_gap + 2)

            if offset > 0 and (offset % 100) == 0:
                time.sleep(relative_time_gap + 3)

        except Exception as e:
            pass
        finally:
            del data


keys = [
    "predict",
]

primary_key = "id"
index_name = "em_news_v5"


def main():
    update_data = db_load()
    print(update_data.head())
    print(update_data.shape)

    print("data_indexing data")
    print("connect to es cloud")
    es = Elasticsearch("dev-server:9200")
    print(f"update data  count {update_data.shape}")

    print("indexing start")

    segments_insert_parallel(
        es, update_data, keys, index_name, update_doc_generator, primary_key
    )

    print("indexing complete")
    es.indices.refresh(index_name)
    es.transport.connection_pool.close()


if __name__ == "__main__":
    main()
