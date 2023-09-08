from prefect import flow, get_run_logger, task
from time import sleep
from random import randint
from prefect_ray.task_runners import RayTaskRunner
import math
from collections import namedtuple

Page = namedtuple("Page", ["page_number", "page_limit", "offset"])


@flow(
    name="UnfinishedRun_ETL",
    description="A flow to reproduce the UnfinishedRun error",
    cache_result_in_memory=False,
    task_runner=RayTaskRunner(),
)
def flow(page_limit: int = 10000, pages_batch_size: int = 10):
    logger = get_run_logger()

    record_count = 15201540
    pages_list = calc_pagination_list_task(
        record_count=record_count, page_limit=page_limit, wait_for=[record_count]
    )
    total_pages = len(pages_list)
    sum_rows = 0

    for start_pages_batch in range(0, len(pages_list), pages_batch_size):
        pages_batch = pages_list[
            start_pages_batch : start_pages_batch + pages_batch_size
        ]
        logger.info(
            f"*** START BATCH: {start_pages_batch}-{start_pages_batch + pages_batch_size}/{total_pages} ***"
        )

        logger.info(
            f"Fetching, processing and uploading {start_pages_batch}-{start_pages_batch + pages_batch_size} pages"
        )
        do_all_futures = do_all.map(pages_batch)
        logger.info("Waiting for futures")
        [_.wait() for _ in do_all_futures]

        uploaded_rows = sum_uploads(do_all_futures, wait_for=[do_all_futures])
        sum_rows += uploaded_rows
        logger.info(
            f"Uploaded {uploaded_rows} on this batch. Uploaded {sum_rows} rows so far"
        )
        logger.info(
            f"*** END BATCH: {start_pages_batch}-{start_pages_batch + pages_batch_size}/{total_pages} ***"
        )


@task(cache_result_in_memory=False)
def calc_pagination_list_task(record_count, page_limit):
    if record_count == 0 or page_limit == 0:
        return []

    total_pages = math.ceil(record_count / page_limit)
    pagination_list = []
    for page_num in range(1, total_pages + 1):
        offset = (page_limit * page_num) - page_limit
        p = Page(page_number=page_num, page_limit=page_limit, offset=offset)
        pagination_list.append(p)
    return pagination_list


@task(cache_result_in_memory=False, retry_delay_seconds=10, retries=2)
def do_all(page_batch):
    sleep(3)
    num = randint(10**6, 10**7)
    return num


@task(cache_result_in_memory=False, retry_delay_seconds=10, retries=2)
def sum_uploads(list_count_uploads):
    return sum(list_count_uploads)


if __name__ == "__main__":
    flow_state = flow(page_limit=100000, pages_batch_size=50)
