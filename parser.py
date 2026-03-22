from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
import requests
import pathlib
import random
import re

from bs4 import BeautifulSoup

try:
    from .config import (
        BASE_URL,
        DETAIL_LINK_SELECTOR,
        NAV_PATH,
        RAW_DIR,
        REQUEST_DELAY,
        REQUEST_RETRIES,
        REQUEST_TIMEOUT,
        USER_AGENT,
        setup_logging,
    )
except ImportError:
    from config import (
        BASE_URL,
        DETAIL_LINK_SELECTOR,
        NAV_PATH,
        RAW_DIR,
        REQUEST_DELAY,
        REQUEST_RETRIES,
        REQUEST_TIMEOUT,
        USER_AGENT,
        setup_logging,
    )

LOGGER = setup_logging("atlas.parser")
LOCAL_OUTPUT_PATH: pathlib.Path | None = None
STAGE1_FILENAME_PATTERN = re.compile(r"epah_list_atlas_projects_(\d{8}T\d{6}Z)\.json$")

# For test
#LOCAL_OUTPUT_PATH = "epah_list_atlas_projects_20260321T194410Z.json"

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
        }
    )
    return session

def build_page_url(source_url: str, page_number: int) -> str:
    parsed = urlparse(source_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page_number)
    return urlunparse(parsed._replace(query=urlencode(query)))

def fetch_page_response(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT,
) -> requests.Response | None:
    if callable(session):
        session = session()
    active_session = session or build_session()
    started_at = datetime.now(timezone.utc)
    LOGGER.info("Fetching URL: %s", url)

    try:
        response = active_session.get(url, timeout=timeout)
        response.raise_for_status()
        time.sleep(random.randint(4, 12))
    except requests.RequestException as exc:
        LOGGER.exception("Failed to fetch URL: %s", url, exc_info=exc)
        return None

    if not response.text.strip():
        LOGGER.error("Empty response body for URL: %s", url)
        return None

    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    LOGGER.info(
        "Fetched URL successfully: %s | status=%s | duration=%.2fs",
        url,
        response.status_code,
        duration,
    )
    return response

class Stage1Scraper:
    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        nav_path: str = NAV_PATH,
        detail_link_selector: str = DETAIL_LINK_SELECTOR,
        raw_dir: Path = RAW_DIR,
        user_agent: str = USER_AGENT,
        request_timeout: float = REQUEST_TIMEOUT,
        request_retries: int = REQUEST_RETRIES,
        request_delay: float = REQUEST_DELAY,
    ) -> None:
        self.base_url = base_url
        self.nav_path = nav_path
        self.detail_link_selector = detail_link_selector
        self.raw_dir = raw_dir
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self.request_retries = request_retries
        self.request_delay = request_delay

    def extract_last_page_number(self, html: str) -> int:
        soup = BeautifulSoup(html, "html.parser")

        nav = soup.select_one('nav.ecl-pagination[aria-label="Pagination"]')
        if not nav:
            LOGGER.info("Pagination block not found, defaulting to a single list page.")
            return 0

        a_tag = nav.select_one("li.ecl-pagination__item--last a")
        if not a_tag:
            LOGGER.info("Last-page link not found, defaulting to the first list page only.")
            return 0

        text = a_tag.get_text(" ", strip=True)
        match = re.search(r"\d+", text)
        if not match:
            LOGGER.info("No page number found in pagination text '%s', defaulting to 0.", text)
            return 0

        last_page_number = int(match.group())
        LOGGER.info(
            "extract_last_page_number() detected last page number %s at %s",
            last_page_number,
            datetime.now().isoformat(timespec="seconds"),
        )
        return last_page_number


    def build_page_url(self, source_url: str, page_number: int) -> str:
        parsed = urlparse(source_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["page"] = str(page_number)
        return urlunparse(parsed._replace(query=urlencode(query)))

    def extract_projects(self,
        html: str,
        *,
        source_url: str,
        source_list_page: int,
    ) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        results: list[dict[str, Any]] = []

        cards = soup.select(
            "div.views-view-responsive-grid.views-view-responsive-grid--horizontal "
            "div.singleProjectItem"
        )

        for card in cards:
            a_tag = card.select_one("div.mapListProjectDetails a[href]")
            if not a_tag:
                continue

            name_tag = a_tag.select_one("strong span")
            project_name = (
                name_tag.get_text(strip=True)
                if name_tag
                else a_tag.get_text(" ", strip=True)
            )
            project_url = urljoin(source_url, a_tag["href"])
            atlas_id = urlparse(project_url).path.split("/")[-1]

            results.append(
                {
                    "project_name": project_name,
                    "atlas_id": atlas_id,
                    "project_url": project_url,
                    "source_list_page": source_list_page,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        LOGGER.info(
            "extract_projects() found %s projects on list page %s",
            len(results),
            source_list_page,
        )
        return results

    def save_projects_to_json(self, all_projects: list[dict[str, Any]], output_path: pathlib.Path) -> pathlib.Path:
    # save into root data/raw/ directory with timestamped filename
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as file_handle:
            json.dump(all_projects, file_handle, ensure_ascii=False, indent=2)
        return output_path


    def get_projects(self, source_url, session, last_page_number):
        all_projects: list[dict[str, Any]] = []
        for page_number in range(last_page_number):
            page_url = self.build_page_url(source_url, page_number)
            response = fetch_page_response(page_url, session=session)
            if not response:
                LOGGER.error("Skipping page %s due to fetch failure.", page_number)
                continue

            projects = self.extract_projects(
                response.text,
                source_url=source_url,
                source_list_page=page_number,
            )
            all_projects.extend(projects)

            time.sleep(random.randint(4, 22))  # Be polite and avoid overwhelming the server
        return all_projects
    
class Stage2Scraper:
    LABEL_MAPPING: dict[str, str] = {
        "Countries impacted": "countries_impacted",
        "Geographical scale": "geographical_scale",
        "Energy poverty phase": "energy_poverty_phase",
        "Intervention type": "intervention_type",
        "Professionals involved": "professionals_involved",
        "Partners involved": "partners_involved",
        "Type of funding": "type_of_funding",
        "Website": "website",
    }

    @staticmethod
    def _normalize_text(value: str) -> str:
        return " ".join(value.replace("\n", " ").split())

    def scrape_title(self, content: str) -> str | None:
        soup = BeautifulSoup(content, "html.parser")
        header = soup.select_one("div.ecl-page-header__info")
        if not header:
            return None

        title_tag = header.select_one("h1.ecl-page-header__title")
        if not title_tag:
            return None

        title = self._normalize_text(title_tag.get_text(" ", strip=True))
        return title or None

    def scrape_scope(self, content: str) -> str | None:
        soup = BeautifulSoup(content, "html.parser")
        scope_span = soup.select_one("div.ecl-content-item-block__title span")
        if not scope_span:
            return None

        scope = self._normalize_text(scope_span.get_text(" ", strip=True))
        return scope or None

    def scrape_project_body(self, content: str) -> str | None:
        soup = BeautifulSoup(content, "html.parser")
        body = soup.select_one("div#projectBody")
        if not body:
            return None

        blocks: list[str] = []
        for element in body.find_all(["p", "ul", "ol"]):
            if element.name == "p":
                paragraph_text = self._normalize_text(element.get_text(" ", strip=True))
                if paragraph_text:
                    blocks.append(paragraph_text)
                continue

            list_items = [
                self._normalize_text(item.get_text(" ", strip=True))
                for item in element.find_all("li")
            ]
            list_items = [item for item in list_items if item]
            if list_items:
                blocks.append("\n".join(f"- {item}" for item in list_items))

        if blocks:
            return "\n\n".join(blocks)

        fallback_text = self._normalize_text(body.get_text(" ", strip=True))
        return fallback_text or None

    def scrape_project_details(self, content: str) -> dict[str, str | None]:
        soup = BeautifulSoup(content, "html.parser")
        details_block = soup.select_one("div#projectDetails")

        details: dict[str, str | None] = {
            normalized_key: None for normalized_key in self.LABEL_MAPPING.values()
        }
        if not details_block:
            return details

        for label, normalized_key in self.LABEL_MAPPING.items():
            strong_tag = details_block.find(
                "strong",
                string=lambda text, expected=label: bool(text)
                and self._normalize_text(text).rstrip(":").lower() == expected.lower(),
            )
            if not strong_tag:
                continue

            value_container = strong_tag.find_parent().find_next_sibling("div")
            if value_container is None:
                value_container = strong_tag.find_next("div")
            if value_container is None:
                continue

            value_text = self._normalize_text(value_container.get_text("; ", strip=True))
            details[normalized_key] = value_text or None

        return details

    @staticmethod
    def _extract_atlas_id_from_url(project_url: str | None) -> str | None:
        if not project_url:
            return None

        atlas_id = urlparse(project_url).path.rstrip("/").split("/")[-1]
        return atlas_id or None

    @classmethod
    def extract_links_from_file(cls, links_file: pathlib.Path | str) -> list[dict[str, str]]:
        links_path = pathlib.Path(links_file)
        with links_path.open("r", encoding="utf-8") as file_handle:
            data = json.load(file_handle)
            links = [
                {
                    "project_url": item["project_url"],
                    "project_name": item.get("project_name"),
                    "atlas_id": item.get("atlas_id")
                    or cls._extract_atlas_id_from_url(item.get("project_url")),
                }
                for item in data
                if "project_url" in item
            ]
        return links

    def parse_links_file(
        self,
        links_file: pathlib.Path | str,
        *,
        session: requests.Session | None = None,
    ) -> list[dict[str, Any]]:
        if callable(session):
            session = session()
        active_session = session or build_session()
        project_links = self.extract_links_from_file(links_file)
        parsed_projects: list[dict[str, Any]] = []

        for link_data in project_links:
            project_link = link_data["project_url"]
            atlas_id = link_data["atlas_id"]
            project_name = link_data["project_name"]
            response = fetch_page_response(project_link, session=active_session)
            if not response:
                LOGGER.error("Skipping project link due to fetch failure: %s", project_link)
                continue

            parsed_project = {
                "atlas_id": atlas_id,
                "project_name": project_name,
                "project_url": project_link,
                "project_title": self.scrape_title(response.text),
                "project_scope": self.scrape_scope(response.text),
                "project_body": self.scrape_project_body(response.text),
                "parsed_at": datetime.now(timezone.utc).isoformat(),
            }
            parsed_project.update(self.scrape_project_details(response.text))
            parsed_projects.append(parsed_project)
            time.sleep(random.randint(4, 16))  # Be polite and avoid overwhelming the server

        return parsed_projects


def save_projects_to_json(all_projects: list[dict[str, Any]], output_path) -> pathlib.Path:
    # save into root data/raw/ directory with timestamped filename
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file_handle:
        json.dump(all_projects, file_handle, ensure_ascii=False, indent=2)
    return output_path


def extract_stage1_timestamp(file_path: pathlib.Path) -> datetime | None:
    match = STAGE1_FILENAME_PATTERN.search(file_path.name)
    if not match:
        return None

    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def load_projects_from_json(input_path: pathlib.Path | str) -> list[dict[str, Any]]:
    path = pathlib.Path(input_path)
    with path.open("r", encoding="utf-8") as file_handle:
        data = json.load(file_handle)
    if isinstance(data, list):
        return data
    raise ValueError(f"Expected a list of projects in {path}.")


def get_project_identity(project: dict[str, Any]) -> str | None:
    atlas_id = project.get("atlas_id")
    if atlas_id:
        return str(atlas_id)

    project_url = project.get("project_url")
    if project_url:
        return str(project_url)

    project_name = project.get("project_name")
    if project_name:
        return str(project_name)

    return None


def filter_new_projects(
    current_projects: list[dict[str, Any]],
    previous_projects: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    previous_project_ids = {
        project_id
        for project in previous_projects
        if (project_id := get_project_identity(project)) is not None
    }
    return [
        project
        for project in current_projects
        if (project_id := get_project_identity(project)) is not None
        and project_id not in previous_project_ids
    ]


def get_latest_stage1_output_path() -> pathlib.Path:
    matches = sorted(RAW_DIR.glob("epah_list_atlas_projects_*.json"))
    if not matches:
        raise FileNotFoundError("No epah_list_atlas_projects_*.json file found in data/raw.")
    return matches[-1]


def runStageOne() -> list[dict[str, Any]]:
    global LOCAL_OUTPUT_PATH
    stage1Parser = Stage1Scraper()
    source_url = BASE_URL
    first_page_response = fetch_page_response(source_url)
    
    if not first_page_response:
        LOGGER.error("Failed to fetch the first page. Aborting stage 1.")
        return []

    active_session = requests.session()
    last_page_number = stage1Parser.extract_last_page_number(first_page_response.text)
    all_projects = stage1Parser.get_projects(source_url, active_session, last_page_number + 1)

    LOGGER.info("Completed parsing. Total projects found: %s", len(all_projects))
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    output_path = RAW_DIR / f"epah_list_atlas_projects_{timestamp}.json"
    LOCAL_OUTPUT_PATH = output_path
    stage1Parser.save_projects_to_json(all_projects, output_path)
    LOGGER.info("Saved parsed projects to: %s", output_path)
    return all_projects


def runStageOneWithControl(min_timestamp_difference_seconds: float) -> list[dict[str, Any]]:
    global LOCAL_OUTPUT_PATH
    previous_stage1_path: pathlib.Path | None = None
    try:
        previous_stage1_path = get_latest_stage1_output_path()
    except FileNotFoundError:
        LOGGER.info("No previous stage 1 file found. Stage 1 will run.")

    if previous_stage1_path is not None:
        last_parse_timestamp = extract_stage1_timestamp(previous_stage1_path)
        if last_parse_timestamp is not None:
            age_seconds = (datetime.now(timezone.utc) - last_parse_timestamp).total_seconds()
            if age_seconds < min_timestamp_difference_seconds:
                LOCAL_OUTPUT_PATH = previous_stage1_path
                LOGGER.info(
                    "Skipping stage 1 because latest parse file is only %.0f seconds old: %s",
                    age_seconds,
                    previous_stage1_path,
                )
                return load_projects_from_json(previous_stage1_path)

        else:
            LOGGER.warning(
                "Could not extract timestamp from previous stage 1 file name: %s. Stage 1 will run.",
                previous_stage1_path,
            )

    current_projects = runStageOne()
    if previous_stage1_path is None or LOCAL_OUTPUT_PATH is None:
        return current_projects

    previous_projects = load_projects_from_json(previous_stage1_path)
    new_projects = filter_new_projects(current_projects, previous_projects)
    save_projects_to_json(new_projects, LOCAL_OUTPUT_PATH)
    LOGGER.info(
        "Filtered stage 1 output down to %s new projects compared with %s",
        len(new_projects),
        previous_stage1_path,
    )
    return new_projects


def runStageTwo() -> None:
    stage2Parser = Stage2Scraper()
    input_path = LOCAL_OUTPUT_PATH or get_latest_stage1_output_path()
    detailed_projects = stage2Parser.parse_links_file(input_path, session=requests.session())
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    output_path = RAW_DIR / f"epah_details_atlas_projects_{timestamp}.json"
    detailed_output_path = save_projects_to_json(detailed_projects, output_path)
    LOGGER.info("Saved detailed parsed projects to: %s", detailed_output_path)


if __name__ == "__main__":
    LOGGER.info("RAW_DIR: %s", RAW_DIR)
    runStageOneWithControl(min_timestamp_difference_seconds=24 * 60 * 60)
    #runStageTwo()
