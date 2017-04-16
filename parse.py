import logging
import argparse
import os
import sqlite3

from datetime import datetime
from bs4 import BeautifulSoup


def main():
    parser = argparse.ArgumentParser(
        description="Parse saved webpages for user and route data."
    )
    parser.add_argument("input",
                        help="The folder with saved .html files. "
                             "This program expects user profiles to be saved "
                             "with filenames in the format <uid>_user.html "
                             "and the corresponding scorecards to be saved in "
                             "the format <uid>_boulders.html")
    parser.add_argument("db_path",
                        help="A path to an existing sqlite3 database. See "
                             "create_tables.sql for the expected schema.")
    parser.add_argument("-l", "--log_level", default="info")
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
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s"
    )

    conn = sqlite3.connect(args.db_path)
    cursor = conn.cursor()

    def get_text_if_exists(element):
        return "" if element is None else element.text

    def parse_user(uid, text):
        soup = BeautifulSoup(text)
        name = soup.find(id="LabelUserName").text
        height = soup.find(id="LabelUserDataHeight").text
        weight = soup.find(id="LabelUserDataWeight").text
        country = soup.find(id="LabelUserCountry").text.strip(", ")
        city = soup.find(id="LabelUserCity").text
        date_string = soup.find(id="LabelUserDataBirth").text
        started_climbing = soup.find(id="LabelUserDataStartedClimbing").text
        occupation = soup.find(id="LabelUserDataOccupation").text
        interests = soup.find(id="LabelUserDataInterrests").text
        best_result = soup.find(id="LabelUserDataBestResult").text
        best_area = soup.find(id="LabelUserDataBestClimbingArea").text
        guide_areas = soup.find(id="LabelUserDataGuide").text
        sponsor = soup.find(id="LabelUserDataLinks").text

        visits_obj = soup.find(id="LabelUserUpdatesVisits")
        # noinspection PyPep8
        presentation_visits = list(visits_obj.children)[0].split(":")[-1].replace(" ", "")
        # noinspection PyPep8
        routes_visits = list(visits_obj.children)[2].split(":")[-1].replace(" ", "")
        # noinspection PyPep8
        boulders_visits = list(visits_obj.children)[4].split(":")[-1].replace(" ", "")
        # noinspection PyPep8
        blog_visits = list(visits_obj.children)[6].split(":")[-1].replace(" ", "")
        # noinspection PyPep8
        total_visits = list(visits_obj.children)[8].text.split(":")[-1].replace(" ", "")

        # Routes rankings
        r_country_score = get_text_if_exists(
            soup.find(id="LabelUserCountryScoreR")
        )
        r_country_ranking = get_text_if_exists(
            soup.find(id="LabelUserCountryRankingR")
        )
        r_world_ranking = get_text_if_exists(
            soup.find(id="LabelUserWorldRankingR")
        )

        r_all_time_country_score = get_text_if_exists(
            soup.find(id="LabelAllTimeUserCountryScoreR")
        )
        r_all_time_country_ranking = get_text_if_exists(
            soup.find(id="LabelAllTimeUserCountryRankingR")
        )
        r_all_time_world_ranking = get_text_if_exists(
            soup.find(id="LabelAllTimeUserWorldRankingR")
        )

        # Boulder rankings
        b_country_score = get_text_if_exists(
            soup.find(id="LabelUserCountryScoreB")
        )
        b_country_ranking = get_text_if_exists(
            soup.find(id="LabelUserCountryRankingB")
        )
        b_world_ranking = get_text_if_exists(
            soup.find(id="LabelUserWorldRankingB")
        )

        b_all_time_country_score = get_text_if_exists(
            soup.find(id="LabelAllTimeUserCountryScoreB")
        )
        b_all_time_country_ranking = get_text_if_exists(
            soup.find(id="LabelAllTimeUserCountryRankingB")
        )
        b_all_time_world_ranking = get_text_if_exists(
            soup.find(id="LabelAllTimeUserWorldRankingB")
        )

        date = datetime.strptime(
            date_string, '%Y-%m-%d'
        ) if date_string != "" else None
        # noinspection PyPep8,PyPep8,PyPep8
        cursor.execute(
            "INSERT INTO Users VALUES "
            "(:uid, :name, :height, :weight, :country, :city, :date,"
            ":started_climbing, :occupation, :other_interests, "
            ":best_comp_result, :best_climbing_area, :guide_areas, :sponsor,"
            ":presentation_visits, :routes_visits, :boulders_visits, "
            ":blog_visits, :total_visits,"
            ":r_country_score, :r_country_ranking_string, "
            ":r_world_ranking_string, :r_all_time_country_score, "
            ":r_all_time_country_ranking_string, "
            ":r_all_time_world_ranking_string,"
            ":b_country_score, :b_country_ranking_string, "
            ":b_world_ranking_string, :b_all_time_country_score,"
            ":b_all_time_country_ranking_string, "
            ":b_all_time_world_ranking_string)",
            {
                "uid": uid, "name": name, "height": height, "weight": weight,
                "country": country, "city": city, "date": date,
                "started_climbing": started_climbing, "occupation": occupation,
                "other_interests": interests, "best_comp_result": best_result,
                "best_climbing_area": best_area, "guide_areas": guide_areas,
                "sponsor": sponsor, "presentation_visits": presentation_visits,
                "routes_visits": routes_visits,
                "boulders_visits": boulders_visits, "blog_visits": blog_visits,
                "total_visits": total_visits,
                "r_country_score": r_country_score,
                "r_country_ranking_string": r_country_ranking,
                "r_world_ranking_string": r_world_ranking,
                "r_all_time_country_score": r_all_time_country_score,
                "r_all_time_country_ranking_string": r_all_time_country_ranking,
                "r_all_time_world_ranking_string": r_all_time_world_ranking,
                "b_country_score": b_country_score,
                "b_country_ranking_string": b_country_ranking,
                "b_world_ranking_string": b_world_ranking,
                "b_all_time_country_score": b_all_time_country_score,
                "b_all_time_country_ranking_string": b_all_time_country_ranking,
                "b_all_time_world_ranking_string": b_all_time_world_ranking
            }
        )
        conn.commit()

    def parse_boulders(uid, text):
        soup = BeautifulSoup(text)
        grade_rows = soup.find_all(class_="AscentListHeadRow")

        for grade_row in grade_rows:
            grade = list(grade_row.find("b").children)[1]
            climbs = grade_row.parent.next_siblings

            for c in climbs:
                if hasattr(c, "children") and len(list(c.children)) == 19:
                    date_obj = list(list(c.children)[1].children)[1]
                    date = datetime.strptime(
                        date_obj.text
                        if hasattr(date_obj, "text")
                        else date_obj,
                        '%y-%m-%d'
                    )

                    type_image = list(c.children)[3].find("img").attrs["src"]
                    # noinspection PyPep8,PyPep8
                    if type_image == "images/56f871c6548ae32aaa78672c1996df7f.gif":
                        type_string = "Flash"
                    elif type_image == "images/979607b133a6622a1fc3443e564d9577.gif":
                        type_string = "Redpoint"
                    else:
                        type_string = "Onsight"

                    climb_name = list(c.children)[5].find("a").text

                    rec_string = list(c.children)[7].find("img").attrs["src"]
                    rec = rec_string == "images/UserRecommended_1.gif"

                    area = list(c.children)[9].text

                    tags = list(c.children)[11].text.strip()

                    comment_elements = list(list(c.children)[13].children)
                    # noinspection PyPep8
                    comment_obj = comment_elements[-1] if len(comment_elements) > 1 else ""
                    # noinspection PyPep8
                    comment = comment_obj.text if hasattr(comment_obj, "text") else comment_obj

                    stars = list(c.children)[15].text.strip().count("*")

                    # noinspection PyPep8
                    cursor.execute(
                        "INSERT INTO Boulders VALUES "
                        "(:uid, :name, :grade, :date, :type, :recommend, :area,"
                        ":tags, :comment, :stars)",
                        {
                            "uid": uid, "name": climb_name, "grade": grade,
                            "date": date, "type": type_string,
                            "recommend": rec, "area": area, "tags": tags,
                            "comment": comment, "stars": stars
                        }
                    )
                    conn.commit()
                elif c == "\n":
                    pass
                else:
                    break

    # Get a list of all the users in the DB, so we don't parse them again.
    cursor.execute("SELECT user_id FROM Users")
    user_ids = set(zip(*cursor.fetchall())[0])

    # Sort them in reverse so all user files are parsed first.
    # TODO: Fix this if necessary.
    for filename in sorted(os.listdir(args.input), reverse=True):
        try:
            if filename.endswith("user.html"):
                u = int(filename.split("_")[0])

                if u in user_ids:
                    logging.info(
                        "Skipping user file {} (uid {})".format(filename, u)
                    )
                else:
                    logging.info(
                        "Parsing user file {} (uid {})".format(filename, u)
                    )

                    with open(os.path.join(args.input, filename)) as f:
                        parse_user(u, f.read())

            elif filename.endswith("boulders.html"):
                u = int(filename.split("_")[0])

                if u in user_ids:
                    logging.info(
                        "Skipping user file {} (uid {})".format(filename, u)
                    )
                else:
                    logging.info(
                        "Parsing boulder file {} (uid {})".format(filename, u)
                    )

                    with open(os.path.join(args.input, filename)) as f:
                        parse_boulders(u, f.read())
            else:
                logging.error("Unrecognized file {}".format(filename))
        except Exception as ex:
            logging.error("Caught exception {}".format(ex))

    conn.close()


if __name__ == "__main__":
    main()
