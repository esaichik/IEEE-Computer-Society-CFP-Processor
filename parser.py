#!/usr/bin/env python3
from csv import DictReader, DictWriter
from datetime import datetime
from enum import StrEnum, Enum
from os import path
from pprint import pprint
from string import printable
from typing import Any

from bs4 import BeautifulSoup, Tag
from requests import get


class DbRecordStatus(StrEnum):
    NEW = "NEW"
    UNMODIFIED = "UNMODIFIED"
    UPDATED = "UPDATED"
    DELETED = "DELETED"


class MediaType(StrEnum):
    MAGAZINE = "Magazine"
    JOURNAL = "Journal"
    CONFERENCE = "Conference"
    UNKNOWN = "Unknown"

    @classmethod
    def from_value(cls, value: str) -> "MediaType":
        if value:
            match value.lower():
                case "magazine":
                    return cls.MAGAZINE
                case "journal":
                    return cls.JOURNAL
                case "conference":
                    return cls.CONFERENCE
                case _:
                    return cls.UNKNOWN
        return cls.UNKNOWN


class DeserializeValueProcessor(Enum):
    MEDIA_TYPE = lambda x: MediaType.from_value(x) if x else MediaType.UNKNOWN
    MEDIA_NAME = lambda x: x
    MEDIA_TITLE = lambda x: x
    MEDIA_DEADLINE = lambda x: datetime.strptime(x, METADATA["DATA_CONTAINER"]['MEDIA_DEADLINE_FORMAT']) \
        if x and METADATA["DATA_CONTAINER"]['MEDIA_DEADLINE_FORMAT'] else None
    MEDIA_TITLE_LINK = lambda x: x
    MEDIA_TITLE_TEXT = lambda x: x.text.strip() if x and x.text else None
    MEDIA_SUMMARY = lambda x: remove_non_printable_chars(x.text.strip()) if x and x.text else None
    MEDIA_ACTIONS_LINK = lambda x: x


TYPE_FIELD_NAME = "Type"
NAME_FIELD_NAME = "Name"
TITLE_FIELD_NAME = "Title"
DEADLINE_FIELD_NAME = "Deadline"

METADATA = {
    "IEEE_CS_CFP_URL": "https://www.computer.org/publications/author-resources/calls-for-papers",
    "HEADERS": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Priority": "u=0, i",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15"
    },
    "DATA_CONTAINER": {
        "CLASS_NAME": "callForPaperPostContainer",
        "TITLE_CLASS_NAME": "callForPaperPostTitle",
        "SUMMARY_CLASS_NAME": "callForPaperPostSummary",
        "ACTION_CLASS_NAME": "callForPaperPostActions",
        "MEDIA_TYPE": "data-callforpaper-type",
        "MEDIA_NAME": "data-publication",
        "MEDIA_DEADLINE": "data-deadline",
        "MEDIA_DEADLINE_FORMAT": "%Y-%m-%d",
    },
    "DB_HEADER": [TYPE_FIELD_NAME, NAME_FIELD_NAME, TITLE_FIELD_NAME, "Summary", DEADLINE_FIELD_NAME, "TitleLink",
                  "ActionsLink"],
    "DB_LOCATION": "",
    "DB_FILENAME": "cfp.db",
    "DB_FIELDS_DELIMITER": ",",
    "DB_ENCODING": "utf-8",
    "DB_RECORDS_ORDER": [DbRecordStatus.NEW, DbRecordStatus.UPDATED, DbRecordStatus.UNMODIFIED]
}


def get_ieee_cs_page() -> BeautifulSoup:
    page = get(METADATA["IEEE_CS_CFP_URL"], headers=METADATA["HEADERS"])
    return BeautifulSoup(page.content, "lxml")


def remove_non_printable_chars(text: str) -> str | None:
    return ''.join(char if char in printable else ' ' for char in text) if text else None


def get_tag_attribute_or_default(tag: Tag, attribute_name: str, default_value=None) -> str | None:
    return tag[attribute_name] if tag.has_attr(attribute_name) and tag[attribute_name] else default_value


def try_extract_name_from_title(title: str) -> str | None:
    if title:
        split = title.split(":")
        return ":".join(split[1:]).strip() if len(split) >= 2 else None
    return None


def create_media_data_dict(media_type: MediaType, name: str, title: str, summary: str, deadline: datetime | None,
                           title_link: str, actions_link: str) -> dict[str, str]:
    if title_link != actions_link:
        print(f"Title link {title_link} does not match actions link {actions_link} for {name}({media_type})")
    header = METADATA["DB_HEADER"]
    return {
        header[0]: media_type,
        header[1]: name,
        header[2]: title,
        header[3]: summary,
        header[4]: deadline,
        header[5]: title_link,
        header[6]: actions_link
    }


def create_composite_key(**data):
    if data[TYPE_FIELD_NAME] is None or data[NAME_FIELD_NAME] is None or data[TITLE_FIELD_NAME] is None:
        print("Unable to create composite key for", data)
        return None
    return data[TYPE_FIELD_NAME] + data[NAME_FIELD_NAME] + data[TITLE_FIELD_NAME]


def process_db_row_data(data: dict[str, Any]) -> dict[str, Any]:
    data[TYPE_FIELD_NAME] = DeserializeValueProcessor.MEDIA_TYPE(data[TYPE_FIELD_NAME])
    data[DEADLINE_FIELD_NAME] = DeserializeValueProcessor.MEDIA_DEADLINE(data[DEADLINE_FIELD_NAME])
    return data


def parse_ieee_cs_cfp_information(page_data: BeautifulSoup) -> dict[str, dict[str, str]] | None:
    result = {}
    try:
        container_metadata = METADATA["DATA_CONTAINER"]
        containers = page_data.find_all("div", class_=container_metadata["CLASS_NAME"])
        for container in containers:
            media_type = DeserializeValueProcessor.MEDIA_TYPE(
                get_tag_attribute_or_default(container, container_metadata["MEDIA_TYPE"]))
            name = DeserializeValueProcessor.MEDIA_NAME(
                get_tag_attribute_or_default(container, container_metadata["MEDIA_NAME"]))
            deadline = DeserializeValueProcessor.MEDIA_DEADLINE(
                get_tag_attribute_or_default(container, container_metadata["MEDIA_DEADLINE"]))
            title_link_element = container.find("div", class_=container_metadata["TITLE_CLASS_NAME"]).find("a")
            title_link = DeserializeValueProcessor.MEDIA_TITLE_LINK(
                get_tag_attribute_or_default(title_link_element, "href"))
            title_text = DeserializeValueProcessor.MEDIA_TITLE_TEXT(
                title_link_element) if title_link_element else None
            summary = DeserializeValueProcessor.MEDIA_SUMMARY(
                container.find("div", class_=container_metadata["SUMMARY_CLASS_NAME"]).find("p"))
            actions_link_element = container.find("div", class_=container_metadata["ACTION_CLASS_NAME"]).find("a")
            actions_link = DeserializeValueProcessor.MEDIA_ACTIONS_LINK(
                get_tag_attribute_or_default(actions_link_element, "href"))
            if name is None:
                name = DeserializeValueProcessor.MEDIA_NAME(try_extract_name_from_title(title_text))
            data = create_media_data_dict(media_type, name, title_text, summary, deadline,
                                          title_link, actions_link)
            key = create_composite_key(**{TYPE_FIELD_NAME: media_type, NAME_FIELD_NAME: name,
                                          TITLE_FIELD_NAME: title_text})
            result[key] = data
        return result
    except BaseException as be:
        print(f"Exception during parsing: {be}")


def match_ieee_cs_cfp_information_with_db(web_data: dict[str, dict[str, Any]]) -> dict[DbRecordStatus, list[dict[
    str, Any]] | None] | None:
    try:
        result = {
            DbRecordStatus.NEW: None,
            DbRecordStatus.UNMODIFIED: None,
            DbRecordStatus.UPDATED: None,
            DbRecordStatus.DELETED: None,
        }
        field_names = METADATA["DB_HEADER"]
        delimiter = METADATA["DB_FIELDS_DELIMITER"]
        db_file_location = path.join(METADATA["DB_LOCATION"], METADATA["DB_FILENAME"])
        encoding = METADATA["DB_ENCODING"]
        if not path.exists(db_file_location):
            result[DbRecordStatus.NEW] = list(web_data.values())
            return result
        else:
            db_data = {}
            with open(db_file_location, "r", encoding=encoding) as csvfile:
                reader = DictReader(csvfile, fieldnames=field_names, delimiter=delimiter)
                # skip headers row
                next(reader)
                for row in reader:
                    deserialized_row = {
                        key: (value if value else None) for key, value in row.items()
                    }
                    composite_key = create_composite_key(**{TYPE_FIELD_NAME: deserialized_row[TYPE_FIELD_NAME],
                                                            NAME_FIELD_NAME: deserialized_row[NAME_FIELD_NAME],
                                                            TITLE_FIELD_NAME: deserialized_row[TITLE_FIELD_NAME]})
                    db_data[composite_key] = process_db_row_data(deserialized_row)
            db_keys = set(db_data.keys())
            web_keys = set(web_data.keys())
            intersected_keys = db_keys & web_keys
            new_keys = web_keys - db_keys
            deleted_keys = db_keys - web_keys
            result[DbRecordStatus.NEW] = [web_data[key] for key in new_keys]
            result[DbRecordStatus.DELETED] = [db_data[key] for key in deleted_keys]
            updated_keys = set()
            for key in intersected_keys:
                if web_data[key] != db_data[key]:
                    updated_keys.add(key)
            result[DbRecordStatus.UPDATED] = [web_data[key] for key in updated_keys]
            result[DbRecordStatus.UNMODIFIED] = [db_data[key] for key in intersected_keys - updated_keys]
            return result
    except BaseException as be:
        print(f"Exception during matching: {be}")


def only_unmodified_records(records: dict[DbRecordStatus, Any]) -> None:
    return records[DbRecordStatus.UNMODIFIED] and not records[DbRecordStatus.UPDATED] and not records[
        DbRecordStatus.DELETED] and not records[DbRecordStatus.NEW]


def update_db_info(records: dict[DbRecordStatus, list[dict[str, Any]]]) -> None:
    if only_unmodified_records(records):
        print("Skipping DB file update as there are only unmodified records")
        return
    try:
        db_file_location = path.join(METADATA["DB_LOCATION"], METADATA["DB_FILENAME"])
        encoding = METADATA["DB_ENCODING"]
        field_names = METADATA["DB_HEADER"]
        delimiter = METADATA["DB_FIELDS_DELIMITER"]
        db_record_status = METADATA["DB_RECORDS_ORDER"]
        container_metadata = METADATA["DATA_CONTAINER"]
        with open(db_file_location, "w", encoding=encoding) as csvfile:
            # It works correctly.
            # noinspection PyTypeChecker
            writer = DictWriter(csvfile, fieldnames=field_names, delimiter=delimiter)
            writer.writeheader()
            for record_status in db_record_status:
                if records[record_status]:
                    ordered_records = sorted(records[record_status], key=lambda record: (
                        record[DEADLINE_FIELD_NAME] is None, record[DEADLINE_FIELD_NAME]))
                    for cfp_record in ordered_records:
                        if cfp_record[DEADLINE_FIELD_NAME]:
                            cfp_record[DEADLINE_FIELD_NAME] = cfp_record[DEADLINE_FIELD_NAME].strftime(
                                container_metadata['MEDIA_DEADLINE_FORMAT'])
                        writer.writerow(cfp_record)
                    print(
                        f"Processed {len(records[record_status])} row{"s" if len(records[record_status]) > 1 else ""} with {record_status} status")

    except BaseException as be:
        print(f"Exception during updating: {be}")


def print_status_information(values_container: dict[DbRecordStatus, list[dict[str, Any]]], status: DbRecordStatus,
                             prefix: str) -> None:
    values = values_container[status]
    if values:
        print(f"{prefix} {len(values)} row{"" if len(values) == 1 else "s"}:")
        for row in values:
            pprint(row)


def main() -> None:
    page = get_ieee_cs_page()
    actual_ieee_cs_cfp_information = parse_ieee_cs_cfp_information(page)
    matching_results = match_ieee_cs_cfp_information_with_db(actual_ieee_cs_cfp_information)
    for key, value in matching_results.items():
        if value:
            print(
                f"There {"is" if len(value) == 1 else "are"} {len(value)} item{"" if len(value) == 1 else "s"} with {key} status")
    update_db_info(matching_results)
    print_status_information(matching_results, DbRecordStatus.NEW, "Added")
    print_status_information(matching_results, DbRecordStatus.DELETED, "Deleted")


if __name__ == "__main__":
    main()
