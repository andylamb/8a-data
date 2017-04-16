import logging
import argparse
import os
import multiprocessing as mp
import Queue
import sqlite3

from selenium import webdriver


def main():
    parser = argparse.ArgumentParser(description="Scrape data from 8a.nu")
    parser.add_argument("-l", "--log_level", default="info")
    parser.add_argument("-s", "--start", type=int, default=1,
                        help="The first user id to parse")
    parser.add_argument("-e", "--end", type=int, default=65000,
                        help="The last user id to parse")
    parser.add_argument("-o", "--output", default="output",
                        help="The directory to save scraped .html files.")
    parser.add_argument("-n", "--n_processes", type=int, default=1)
    parser.add_argument("db_path",
                        help="A path to an existing sqlite3 database. See "
                             "create_tables.sql for the expected schema.")
    args = parser.parse_args()

    levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }
    level = levels.get(args.log_level, logging.NOTSET)
    logging.basicConfig(
        level=level,
        format="%(process)d:%(asctime)s:%(levelname)s:%(name)s:%(message)s"
    )

    os.system("mkdir -p {}".format(args.output))

    conn = sqlite3.connect(args.db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT user_id FROM ScrapeExceptions")
    exception_ids = zip(*cursor.fetchall())[0]

    def get_users():
        driver = webdriver.Firefox()

        while True:
            try:
                uid = q.get(block=False)
            except Queue.Empty:
                driver.close()
                break

            user_path = os.path.join(
                args.output, "{}_user.html".format(uid)
            )
            boulders_path = os.path.join(
                args.output, "{}_boulders.html".format(uid)
            )
            if os.path.exists(user_path) and os.path.exists(boulders_path):
                logging.info(
                    "Files {} and {} already exist, skipping.".format(
                        user_path, boulders_path
                    )
                )
            elif uid in exception_ids:
                logging.info(
                    "Uid {} in ScrapeExceptions, skipping.".format(uid)
                )
            else:
                url = "https://www.8a.nu/User/Profile.aspx?UserId={}".format(
                    uid
                )
                logging.info("Scraping {}.".format(url))
                driver.get(url)
                try:
                    driver.switch_to.frame(driver.find_element_by_id("main"))

                    with open(user_path, 'w') as f:
                        f.write(driver.page_source.encode('utf-8'))
                        logging.info("Saved user to {}".format(user_path))

                    driver.find_element_by_link_text("Boulders").click()
                    driver.find_element_by_partial_link_text(
                        "All Ascents"
                    ).click()

                    with open(boulders_path, 'w') as f:
                        f.write(driver.page_source.encode('utf-8'))
                        logging.info(
                            "Saved boulders to {}".format(boulders_path)
                        )

                    logging.info("Scraped {}".format(url))
                except Exception as ex:
                    logging.warn(
                        "Could not parse request for {} "
                        "(caught exception {}).".format(url, ex)
                    )

                    cursor.execute(
                        "INSERT INTO ScrapeExceptions VALUES (:uid, :ex)",
                        {"uid": uid, "ex": str(ex)}
                    )
                    conn.commit()

    processes = [mp.Process(target=get_users) for _ in range(args.n_processes)]
    q_size = args.end - args.start
    logging.info("Creating queue of size {}".format(q_size))
    q = mp.Queue(q_size)
    logging.info(
        "Loading range [{}, {}) into the queue.".format(args.start, args.end)
    )
    for u in range(args.start, args.end):
        q.put(u)

    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
